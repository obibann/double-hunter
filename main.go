package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"
)

/**
*	Global variables
**/
var REALEXEC = true
var DEBUG = false
var RECURSIVE = false
var VERBOSE = true
var SEARCH_PATH = ""
var DOUBLE_PATH = ""
var NB_THREADS = 8
var HASH_CHAN = make([]chan fileObject, NB_THREADS)
var FLAT_FILE_LIST []fileObject
var SORTED_FILE_LIST = make(map[string][]fileObject)
var UPDATED_FILES = 0
var PATH = getWorkingDirPath()
var IGNORE_EMPTY = false

/**
*	File object struct
**/
type fileObject struct {
	location string
	size     int64
	hash     string
}

/**
*	log
*	@param log function
*	@param message: string
*	@param severity: string (info, error, debug)
**/
func log(message string, severity string) {

	if (DEBUG && severity == "debug") || (severity != "debug") {
		dt := time.Now()
		fd := fmt.Sprintf("%02d-%02d-%d %02d:%02d:%02d", dt.Day(), dt.Month(), dt.Year(), dt.Hour(), dt.Minute(), dt.Second())
		fmt.Printf("[%s][%s] %s\n", fd, severity, message)
	}

}

/**
*	getWorkingDirPath
**/
func getWorkingDirPath() string {

	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	fmt.Println("workingDirPath:", dir)
	return dir

}

/**
*	getExecFreeSlot
**/
func getExecFreeSlot() int {

	for i, c := range HASH_CHAN {
		if c == nil {
			return i
		}
	}

	return -1

}

/**
*	initJobQueue
**/
func initJobQueue() {
	HASH_CHAN = make([]chan fileObject, NB_THREADS)
}

/**
*	registerHashJob
*	@param index
*	@param channel
**/
func registerHashJob(index int, channel chan fileObject) {
	HASH_CHAN[index] = channel
}

/**
*	freeExecSlot
*	@param index
**/
func freeExecSlot(index int) {
	HASH_CHAN[index] = nil
}

/**
*	calculateHash
**/
func calculateHash() {

	initJobQueue()
	go displayHashPercentage(false)
	for _, file := range FLAT_FILE_LIST {
		parrallelHash(file)
	}
	waitCompleteJob()
	displayHashPercentage(true)

}

/**
*	createHashJob
*	@param file
*	@param slot
**/
func createHashJob(file fileObject, slot int) {

	log(fmt.Sprintf("Slot %d: %s", slot, file.location), "debug")
	channel := make(chan fileObject)
	go hashFile(file, channel)
	registerHashJob(slot, channel)

}

/**
*	waitCompleteJob
**/
func waitCompleteJob() {

	for i, c := range HASH_CHAN {
		if c != nil {
			execResult, ok := <-c
			if ok {
				updateHash(execResult)
				log(fmt.Sprintf("Freeing slot %d - %s", i, execResult.hash), "debug")
				freeExecSlot(i)
			}
		}
	}

}

/**
*	parrallelHash
*	@param file
**/
func parrallelHash(file fileObject) {

	for {
		slot := getExecFreeSlot()
		if slot >= 0 {
			createHashJob(file, slot)
			break
		} else {
			waitCompleteJob()
		}
	}

}

/**
*	addFile
*	@param file
**/
func addFile(file fileObject) {
	FLAT_FILE_LIST = append(FLAT_FILE_LIST, file)
}

/**
*	displayHashPercentage
**/
func displayHashPercentage(once bool) {

	for {
		pct := float32(float32(UPDATED_FILES) / float32(len(FLAT_FILE_LIST)) * 100.00)
		log(fmt.Sprintf("%.02f%%", pct), "info")
		time.Sleep(5 * time.Second)
		if pct >= 100 || once {
			break
		}
	}

}

/**
*	updateHash
*	@param file
**/
func updateHash(file fileObject) {

	for i, f := range FLAT_FILE_LIST {
		if f.location == file.location {
			log(fmt.Sprintf("Updated %s => %s", file.location, file.hash), "debug")
			FLAT_FILE_LIST[i] = file
			UPDATED_FILES++
			break
		}
	}

}

/**
*	hashFile
*	@param file
*	@param channel
**/
func hashFile(file fileObject, channel chan fileObject) {

	md5Hash := ""

	f, err := os.Open(file.location)

	if err != nil {
		log(err.Error(), "error")
		channel <- file
		//close(channel)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		log(err.Error(), "error")
		channel <- file
		//close(channel)
	}

	md5Hash = fmt.Sprintf("%x", h.Sum(nil))
	file.hash = md5Hash
	log(fmt.Sprintf("%s // %s", file.location, file.hash), "debug")

	channel <- file
	close(channel)

}

/**
*	sortFiles
**/
func sortFiles() {

	for _, f := range FLAT_FILE_LIST {

		log(fmt.Sprintf("%s - %s", f.location, f.hash), "debug")

		if len(f.hash) > 0 {
			entry := SORTED_FILE_LIST[f.hash]
			entry = append(entry, f)
			SORTED_FILE_LIST[f.hash] = entry
		}

	}

}

/**
*	detectDoubles
**/
func detectDoubles() {

	doubles := 0
	for h, files := range SORTED_FILE_LIST {
		if len(files) > 1 {
			doubles++
			if VERBOSE {
				log(fmt.Sprintf("File with hash %s is not unique:", h), "info")
			}
			toKeep := ""
			for _, f := range files {
				if (len(toKeep) == 0) || (len(toKeep) > len(f.location)) {
					toKeep = f.location
				}
			}
			for _, f := range files {
				if f.location != toKeep {
					if VERBOSE {
						log(fmt.Sprintf("  - %s [deleted]", f.location), "info")
					}
					if REALEXEC {
						deleteFile(f)
					}
				} else {
					if VERBOSE {
						log(fmt.Sprintf("  - %s [kept]", f.location), "info")
					}
				}
			}
		}
	}

	if doubles == 0 {
		log("No doubles detected", "info")
	}

}

/**
*	listFiles
**/
func listFiles(location string) {

	files, err := ioutil.ReadDir(location)

	if err == nil {

		for _, file := range files {

			fileLocation := fmt.Sprintf("%s%s%s", location, string(os.PathSeparator), file.Name())

			if file.IsDir() && RECURSIVE {
				listFiles(fileLocation)
			}
			if !file.IsDir() {
				if file.Size() == 0 && IGNORE_EMPTY {
					continue
				}
				currentFile := fileObject{location: fileLocation, size: file.Size(), hash: ""}
				addFile(currentFile)
			}

		}

	}

}

/**
*	deleteFile
*	@param file
**/
func deleteFile(file fileObject) {
	err := os.Remove(file.location)
	if err != nil {
		log(fmt.Sprintf("Could not remove %s: %s", file.location, err.Error()), "error")
	}
}

/**
*	Main
**/
func main() {

	flag.Usage = func() {
		flag.PrintDefaults()
		fmt.Printf("  -h: print help\n")
	}

	// flags declaration using flag package
	flag.IntVar(&NB_THREADS, "t", 8, "Number of file hash calculation in parallel")
	flag.BoolVar(&REALEXEC, "e", false, "Disables simulation mode. Deletes file for real.")
	flag.BoolVar(&DEBUG, "d", false, "Enables debug mode")
	flag.BoolVar(&RECURSIVE, "r", false, "Read files recursively")
	flag.BoolVar(&IGNORE_EMPTY, "i", false, "Ignore empty files")
	flag.StringVar(&PATH, "p", PATH, "Defines location to search doubles in")
	flag.Parse()

	if !REALEXEC {
		log("Simulation mode is ACTIVE", "info")
	} else {
		log("Simulation mode is INACTIVE", "warning")
	}

	log(fmt.Sprintf("Listing all files in %s...", PATH), "info")
	listFiles(PATH)

	log("Calculating hashes... This may take some time, it's the right moment to have a coffee :)", "info")
	calculateHash()

	log("Sorting file list ...", "info")
	sortFiles()

	log("Detecting doubles ...", "info")
	detectDoubles()

}
