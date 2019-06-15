package main

import (
  "os"
  "log"
  "strconv"
  "strings"
  "runtime"
  "path"
  "path/filepath"
  "io/ioutil"
  "bufio"
  "time"
  "sync"
)

const Out = "out"
var tags = make(map[string]bool)
var wg sync.WaitGroup

func main() {
  start := time.Now()
  log.Printf("Start Time %s", start)
  maxIdx := 10
  mode := int(0777)
  cwd := getCWD()
  _ = os.Mkdir(filepath.Join(cwd, Out), os.FileMode(mode))

  for i := 1; i <= maxIdx; i++ {
    folderPath := filepath.Join(cwd, strconv.Itoa(i))
    if _, err := os.Stat(folderPath); err == nil {
      if os.IsNotExist(err) {
        log.Printf("stat error [%v]\n", err)
        break
      }
      log.Printf(folderPath+"\n")
      wErr := filepath.Walk(folderPath, func(path string, info os.FileInfo, wErr error) error {
        if info.IsDir() {
          return nil
        }
        if filepath.Ext(path) == ".csv" {
          tags = make(map[string]bool)
          wg.Add(1)
          go ProcessFile(path)
          wg.Wait()
        }
        return nil
      })
      if wErr != nil {
        log.Printf("walk error [%v]\n", wErr)
      }
    }
  }
  elapsed := time.Since(start)
  log.Printf("Time taken %s", elapsed)
}

func getCWD() (_path string){
  _, filename, _, ok := runtime.Caller(0)
  if !ok {
      panic("No caller information")
  }
  _path = path.Dir(filename)
  return 
}

func ProcessFile(_file string) {
  defer wg.Done()
  log.Printf("---------------------------")
  log.Printf("Memory Usage Pre-processing")
  PrintMemUsage()
  log.Printf("Now processing %s", _file)
  file, err := os.Open(_file)
  if err != nil {
      log.Fatal(err)
  }
  defer file.Close()

  dataToWrite := ""
  prevTag := ""
  scanner := bufio.NewScanner(file)
  totalLines := 0
  extractedLines := 0
  lineForThisTag := 0
  for scanner.Scan() {
    totalLines++
    line := scanner.Text()
    _tag := strings.Split(line, ",")[0]
    line = line+"\n"
    if tags[_tag] == true {
      dataToWrite += line
      lineForThisTag++
    } else {
      tags[_tag] = true
      if prevTag != "" {
        _tagFileName := filepath.Join(getCWD(), "out", prevTag+".csv")
        AppendOrCreateFile(_tagFileName, dataToWrite)
        log.Printf("Tag [%s] has [%d] records", prevTag, lineForThisTag)
        extractedLines += lineForThisTag
      }
      dataToWrite = line
      prevTag = _tag
      lineForThisTag = 1
    }
  }

  if err := scanner.Err(); err != nil {
      log.Fatal(err)
  }

  _tagFileName := filepath.Join(getCWD(), "out", prevTag+".csv")
  log.Printf("Tag [%s] has [%d] records", prevTag, lineForThisTag)
  extractedLines += lineForThisTag
  if(!Exists(_tagFileName)) {
    _err := ioutil.WriteFile(_tagFileName, []byte(dataToWrite), 0777)
    if _err != nil {
      log.Printf("Unable to write file: %v", _err)
    }
  } else {
    AppendOrCreateFile(_tagFileName, dataToWrite)
  }
  log.Printf("Total records : %d", totalLines)
  log.Printf("Extracted records : %d", extractedLines)
  log.Printf("Processing DONE \n")
  log.Printf("Memory Usage Post-processing")
  PrintMemUsage()
  log.Printf("---------------------------")
  runtime.GC()
}

func Exists(name string) bool {
  if _, err := os.Stat(name); err != nil {
      if os.IsNotExist(err) {
          return false
      }
  }
  return true
}

func AppendOrCreateFile(fileName string, line string) {     
  file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
  if err != nil {
    log.Fatalf("failed opening file: %s", err)
  }
  defer file.Close()

  _, err = file.WriteString(line)
  if err != nil {
      log.Fatalf("failed writing to file: %s", err)
  }
}

func PrintMemUsage() {
  var m runtime.MemStats
  runtime.ReadMemStats(&m)
  log.Printf("Alloc = %v MiB", bToMb(m.Alloc))
  log.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
  log.Printf("\tSys = %v MiB", bToMb(m.Sys))
  log.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
  return b / 1024 / 1024
}