package main

import "errors"
import "flag"
import "fmt"
import "log"
import "os"
import "os/exec"
import "path/filepath"
import "strings"

func getSparkHome() (sparkHome string, err error) {
	var sparkHomeEnv = os.Getenv("SPARK_HOME")
	if sparkHomeEnv != "" {
		sparkHome = sparkHomeEnv
	} else {
		maprHome := os.Getenv("MAPR_HOME")
		if maprHome != "" {
			sparkVersionFileLocation := filepath.Join(maprHome, "spark/sparkversion")
			sparkVersionBytes, e := os.ReadFile(sparkVersionFileLocation)
			if e == nil {
				var sparkVersion = string(sparkVersionBytes)
				sparkVersion = strings.Trim(sparkVersion, " \n")
				if sparkVersion != "" {
					sparkHome = filepath.Join(maprHome, fmt.Sprintf("spark/spark-%s", sparkVersion))
				}
			}
		}
		if sparkHome == "" {
			sparkHome, _ = os.Readlink("/usr/local/spark")
		}
	}

	if sparkHome == "" {
		err = errors.New("Can not find SPARK_HOME!")
	}
	return sparkHome, err
}

func getBlacklist(blacklistFileLocation string) (blacklist []string, err error) {
	blacklistBytes, err := os.ReadFile(blacklistFileLocation)
	if err != nil {
		return blacklist, fmt.Errorf("Can not read dep-blacklist.txt configuration file: %s\n", err)
	}
	blacklistString := string(blacklistBytes)
	blacklist = strings.Fields(blacklistString)

	return blacklist, nil
}

func parseClasspathString(classpathString string) []string {
	classpathSeparator := func(c rune) bool {
		return c == ':'
	}
	return strings.FieldsFunc(classpathString, classpathSeparator)
}

func getMaprClasspath() (maprClasspath []string, err error) {
	maprClasspathCmd := exec.Command("mapr", "classpath")
	maprClasspathOutput, err := maprClasspathCmd.Output()
	if err != nil {
		return maprClasspath, fmt.Errorf("Error executing 'mapr classpath' command: %s\n", err)
	}

	var maprClasspathString = string(maprClasspathOutput)
	maprClasspathString = strings.Trim(maprClasspathString, " \n")
	if maprClasspathString == "" {
		return maprClasspath, errors.New("Output of 'mapr classpath' command is empty!")
	}

	maprClasspath = parseClasspathString(maprClasspathString)

	return maprClasspath, nil
}

type Classpath struct {
	entries            []string
	_blacklistDict     map[string]bool
	_blacklistPatterns []string
}

func NewClasspath() *Classpath {
	cp := new(Classpath)
	cp.entries = make([]string, 0, 2048)
	cp._blacklistDict = make(map[string]bool, 256)
	cp._blacklistPatterns = make([]string, 256)

	return cp
}

func (cp *Classpath) SetBlacklist(blacklist []string) {
	isPattern := func(path string) bool {
		magicChars := `*?[\`
		return strings.ContainsAny(path, magicChars)
	}

	for _, entry := range blacklist {
		if isPattern(entry) {
			cp._blacklistPatterns = append(cp._blacklistPatterns, entry)
		} else {
			cp._blacklistDict[entry] = true
		}
	}
}

func (cp *Classpath) IsBlacklisted(entry string) bool {
	if _, found := cp._blacklistDict[entry]; found {
		return true
	}
	for _, pattern := range cp._blacklistPatterns {
		if matched, _ := filepath.Match(pattern, entry); matched {
			return true
		}
	}

	return false
}

func (cp *Classpath) HandleWildcard(entry string) (entries []string) {
	expand := false

	entryLocation := strings.TrimSuffix(entry, "*")

	files, err := os.ReadDir(entryLocation)
	if err != nil {
		// log.Printf("Can not read '%s' directory: %s\n", entryLocation, err)

		// If we can't read it then lets leave it as is:
		return cp.HandleLocation(entry)
	}
	for _, file := range files {
		filename := file.Name()
		fullpath := filepath.Join(entryLocation, filename)
		if cp.IsBlacklisted(fullpath) {
			expand = true
		} else {
			entries = append(entries, fullpath)
		}
	}

	if expand {
		return entries
	}

	return cp.HandleLocation(entry)
}

func (cp *Classpath) HandleLocation(entry string) (entries []string) {
	if !cp.IsBlacklisted(entry) {
		entries = append(entries, entry)
	}

	return entries
}

func (cp *Classpath) AsString() string {
	return strings.Join(cp.entries, ":")
}

func classpathFilter(classpath []string, blacklist []string) string {
	cp := NewClasspath()
	cp.SetBlacklist(blacklist)

	for _, classpathEntry := range classpath {
		var entries []string
		if strings.HasSuffix(classpathEntry, "/*") {
			entries = cp.HandleWildcard(classpathEntry)
		} else {
			entries = cp.HandleLocation(classpathEntry)
		}
		cp.entries = append(cp.entries, entries[:]...)
	}

	return cp.AsString()
}

func main() {
	flag.Usage = func() {
		filename := filepath.Base(os.Args[0])
		msg := "Usage of %s: [<CLASSPATH>]\n" +
			"  -b path\n" +
			"    path to blacklist configuration (default \"$SPARK_HOME/conf/dep-blacklist.txt\")\n" +
			"\n" +
			"Filter mapr classpath using rules from dep-blacklist.txt.\n"
		fmt.Printf(msg, filename)
	}
	blacklistLocationArg := flag.String("b", "", "path to blacklist configuration")
	flag.Parse()

	args := flag.Args()

	var classpathArg string
	if len(args) > 1 {
		fmt.Println("invalid number of arguments")
		flag.Usage()
		os.Exit(1)
	} else if len(args) == 1 {
		classpathArg = args[0]
	}

	var blacklistFileLocation string
	if *blacklistLocationArg != "" {
		blacklistFileLocation = *blacklistLocationArg
	} else {
		sparkHome, err := getSparkHome()
		if err != nil {
			log.Fatalln(err)
		}
		blacklistFileLocation = filepath.Join(sparkHome, "conf/dep-blacklist.txt")
	}

	blacklist, err := getBlacklist(blacklistFileLocation)
	if err != nil {
		log.Fatalln(err)
	}

	var maprClasspath []string
	if classpathArg != "" {
		maprClasspath = parseClasspathString(classpathArg)
	} else {
		maprClasspath, err = getMaprClasspath()
		if err != nil {
			log.Fatalln(err)
		}
	}

	classpath := classpathFilter(maprClasspath, blacklist)
	fmt.Print(classpath)
}
