package orfs

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/howeyc/crc16"
	"os"
	"strconv"
	"strings"
	"time"
)

var MdEntryTooShort = fmt.Errorf("Metadata entry too short")
var MdEntryEmpty = fmt.Errorf("Metadata entry empty")
var MdEntryInvalid = fmt.Errorf("Metadata entry is invalid")

func makeMdEntryNewline(state byte, f OrfsStat) []byte {
	ret := makeMdEntry(state, f)
	return append(ret, "\n"...)
	//ret := []byte("\n")
	//return append(ret, makeMdEntry(state, f)...)
}

func makeMdEntry(state byte, f OrfsStat) []byte {
	entry := []byte{}
	// write out state first.
	entry = append(entry, state)
	if f.IsDir() {
		entry = append(entry, 'd')
	} else {
		entry = append(entry, 'f')
	}
	// Encode len(f.name) in the byte slice as uint16
	entry = append(entry, ';')
	entry = append(entry, []byte(strconv.FormatUint(uint64(len(f.Name())), 10))...)
	// write out f.name into byte slice
	entry = append(entry, ';')
	entry = append(entry, []byte(f.Name())...)
	// Encode f.size (int64) in the byte slice as uint64
	entry = append(entry, ';')
	entry = append(entry, []byte(strconv.FormatUint(uint64(f.Size()), 10))...)
	// Encode f.modTime.Unix() in the byte slice as uint64
	entry = append(entry, ';')
	entry = append(entry, []byte(strconv.FormatUint(uint64(f.ModTime().Unix()), 10))...)
	// Add inode (uuid)
	entry = append(entry, ';')
	entry = append(entry, uuid.UUID(f.Inode()).String()...)

	// Calculate checksum and write it out.
	crc := crc16.ChecksumCCITT(entry)
	entry = append(entry, ';')
	entry = append(entry, []byte(strconv.FormatUint(uint64(crc), 16))...)
	return entry
}

func findNext(p []byte, del byte, start int) int {
	for i := start; i < len(p); i++ {
		if p[i] == del {
			return start + i
		}
	}
	return -1
}

func parseMdEntry(entry []byte) (byte, OrfsStat, error) {
	// Check if entry is empty
	if len(entry) == 0 {
		return 0x0, nil, MdEntryEmpty
	}
	// Shortest possible MD Entry length is 12 bytes
	if len(entry) < 12 {
		return 0x0, nil, MdEntryTooShort
	}
	pos := 0
	etype := entry[pos:findNext(entry, ';', 0)]
	fmt.Printf("Len etype: %v, etype: %v\n", len(etype), etype)
	if len(etype) != 2 {
		panic(MdEntryInvalid)
		return 0x0, nil, MdEntryInvalid
	}
	pos += len(etype) + 1
	state := etype[0]
	isDir := etype[1] == 'd'
	fmt.Printf("ParseMDEntry, is dir: %v\n", isDir)

	// read out filename length
	nLengthBytes := entry[pos : findNext(entry, ';', pos)-pos]
	nLength, err := strconv.ParseUint(string(nLengthBytes), 10, 64)
	if err != nil {
		fmt.Printf("nlength bytes: %v, string: %v, err: %v\n", nLengthBytes, string(nLengthBytes), err)
		panic(MdEntryInvalid)
		return 0x0, nil, MdEntryInvalid
	}
	pos += len(nLengthBytes) + 1
	// Read filename of length
	fName := entry[pos : uint64(pos)+nLength]
	pos += len(fName) + 1

	// Read out filesize
	fsizeBytes := entry[pos : findNext(entry, ';', pos)-pos]
	fsize, err := strconv.ParseUint(string(fsizeBytes), 10, 64)
	if err != nil {
		fmt.Printf("fsizeBytes bytes: %v, string: %v, err: %v\n", fsizeBytes, string(fsizeBytes), err)
		panic(MdEntryInvalid)
		return 0x0, nil, MdEntryInvalid
	}
	pos += len(fsizeBytes) + 1

	// Read out modtime
	modTimeBytes := entry[pos : findNext(entry, ';', pos)-pos]
	modTime, err := strconv.ParseUint(string(modTimeBytes), 10, 64)
	if err != nil {
		panic(MdEntryInvalid)
		return 0x0, nil, MdEntryInvalid
	}
	pos += len(modTimeBytes) + 1

	// Read out file inode (uuid)
	inode, err := uuid.Parse(string(entry[pos : pos+36]))
	if err != nil {
		return 0x0, nil, err
	}
	pos += 36 + 1

	// Read out crc
	crcBytes := entry[pos:]
	crc, err := strconv.ParseUint(string(crcBytes), 16, 16)
	if err != nil {
		fmt.Printf("crcBytes: %v, string: %v, err: %v\n", crcBytes, string(crcBytes), err)
		panic(MdEntryInvalid)
		return 0x0, nil, MdEntryInvalid
	}

	// Calculate crc
	crcCalc := crc16.ChecksumCCITT(entry[0 : pos-1])
	if uint16(crc) != crcCalc {
		panic(MdEntryInvalid)
		return 0x0, nil, MdEntryInvalid
	}

	fileMode := os.FileMode(0000)
	if isDir {
		fileMode = os.FileMode(0755)
	} else {
		fileMode = os.FileMode(0644)
	}

	f := Istat{
		isDir:   isDir,
		modTime: time.Unix(int64(modTime), 0),
		mode:    fileMode,
		name:    string(fName),
		size:    int64(fsize),
		sys:     nil,
	}
	copy(f.inode[:], inode[:16])
	fmt.Printf("ParseMDEntry, Stat isdir: %v\n", f.IsDir())
	return state, &f, nil

}

func (f *fsObj) ReadMD() error {
	buf := make([]byte, 1024*1024*4) // should make this a loop and parse stuff as i go..
	pos := uint64(0)

	for {
		n, err := f.fs.mdctx.Read(f.Inode().String(), buf, pos)
		if err != nil {
			fmt.Fprintf(f.fs.debuglog, "Failed to read inode: %v, error: %v\n", f.Inode().String(), err)
			return err
		}
		mdEntries := strings.Split(string(buf[:n]), "\n")
		for _, entry := range mdEntries {
			status, stat, err := parseMdEntry([]byte(entry))
			if err == MdEntryEmpty {
				continue
			} else {
				fmt.Fprintf(f.fs.debuglog, "Failed to parse MD entry, entry: %v, error: %v\n", entry, err)
			}
			if status == '+' {
				fmt.Fprintf(f.fs.debuglog, "Readdir on: %v, adding %v, isdir: %v\n", f.Inode().String(), stat.Name(), stat.IsDir())
				if stat.IsDir() {
					f.childDir[stat.Inode()] = &fsObj{
						name:    stat.Name(),
						size:    stat.Size(),
						mode:    stat.Mode(),
						modTime: stat.ModTime(),
						isDir:   stat.IsDir(),
						inode:   stat.Inode(),
					}
				} else {
					f.childObj[stat.Inode()] = &fsObj{
						name:    stat.Name(),
						size:    stat.Size(),
						mode:    stat.Mode(),
						modTime: stat.ModTime(),
						isDir:   stat.IsDir(),
						inode:   stat.Inode(),
					}
				}
			} else if status == '-' {
				fmt.Fprintf(f.fs.debuglog, "Readdir on: %v, removing %v\n", f.Inode().String(), stat.Name())
				if stat.IsDir() {
					delete(f.childDir, stat.Inode())
				} else {
					delete(f.childObj, stat.Inode())
				}
			} else {
				return fmt.Errorf("Weird status: %v for entry: %v\n", status, entry)
			}
		}
		f.lastRead = time.Now()

		if n == len(buf) {
			pos += uint64(n)
		} else {
			break
		}

	}
	return nil
}
