package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const MAXDET = 5000000000000
const UBPATH = "/media/sf_work/"
const OUTPATH = "/media/sf_work/ubout/"

type F struct {
	f  *os.File
	gf *gzip.Writer
	fw *bufio.Writer
}
type FileInfo struct {
	fields       string
	keys         string
	outputfields string
	keepflds     [500]int
	reffile      bool
	bpfile       bool
	cyclefile    bool
	subcyclefile bool
	summaryfile  bool
	aggridx      int
	svcarridx    int
	pfidx        int
	ptidx        int
	cgidx        int
	tcidx        int
	svcprdidx    int
	invnumidx    int
	summaryfiles string
	sumhdr       string
}

var fileMap = make(map[string]FileInfo)
var subcycles []string
var billperiod string

func main() {

	//	var test_map = make(map[string]string)
	//  _ = refMap(getFile("/media/ubuntu3/SeagateBackupPlus/work/ubbase/MAF_TRANCODE/20170105/06/32520000/SUB_FTRANCODE_ID1_T20150607_C00_SC00_00.DAT.gz"), test_map)

	billcycle := ""
	subcycle := ""

	//	  billperiod = "T20161119"        //VERIFIED
	//		billperiod = "T20161120"    //VERIFIED
	//		billperiod = "T20161122"    //VERIFIED
	//		billperiod = "T20161123"    //VERIFIED
	//	 	billperiod = "T20161125"    //VERIFIED
	//	 	billperiod = "T20161126"    //VERIFIED
	//		billperiod = "T20161128"    //VERIFIED
	//	 	billperiod = "T20161129"    //VERIFIED
	//	  billperiod = "T20161201"      //VERIFIED
	//	  billperiod = "T20161202"        //VERIFIED
	//	 	billperiod = "T20161204"       //VERIFIED
	//		billperiod = "T20161205"     //VERIFIED
	//	 	billperiod = "T20161207"    //VERIFIED
	//	 	billperiod = "T20161208"        //VERIFIED
	//	  billperiod = "T20161210"       //VERIFIED
	//	 	billperiod = "T20161211"      //VERIFIED
	//   	billperiod = "T20161213"      //VERIFIED
	//  	billperiod = "T20161214"      //VERIFIED
	//		billperiod = "T20161215"      //VERIFIED
	//		billperiod = "T20161216"     //VERIFIED
	//		billperiod = "T20161217"     //VERIFIED
	//		billperiod = "T20161218"     //VERIFIED
	//		billperiod = "T20161219"     //VERIFIED
	//		billperiod = "T20161220"     //VERIFIED
	//		billperiod = "T20161222"     //VERIFIED
	//		billperiod = "T20161223"     //VERIFIED
	//		billperiod = "T20161225"     //VERIFIED
	//		billperiod = "T20161226"     //VERIFIED
	//		billperiod = "T20161228"     //VERIFIED
	//		billperiod = "T20161229"     //VERIFIED
	//    billperiod = "T20170101"     //VERIFIED
	//		  billperiod = "T20170102"     //VERIFIED
	//			billperiod = "T20170104"     //VERIFIED
	//			billperiod = "T20170105"     //VERIFIED
	//	 	billperiod = "T20170107"     //VERIFIED
	//	 	billperiod = "T20170108"     //VERIFIED
	//	  billperiod = "T20170110"     //VERIFIED
	//		 	billperiod = "T20170111"     //VERIFIED
	//	 	 	billperiod = "T20170113"     //VERIFIED
	//    billperiod = "T20170114" //VERIFIED
	//	billperiod = "T20170115" //VERIFIED
	//	 	billperiod = "T20170116" //VERIFIED
	//	  billperiod = "T20170117" //VERIFIED
	//	  billperiod = "T20170118" //VERIFIED
	//		billperiod = "T20170119" //VERIFIED
	//		billperiod = "T20170120"     //VERIFIED
	//		billperiod = "T20170122"     //VERIFIED
	//		billperiod = "T20170123"     //VERIFIED
	//		billperiod = "T20170125"     //VERIFIED
	//		billperiod = "T20170126"     //VERIFIED
	//		billperiod = "T20170128"    //VERIFIED
//	billperiod = "T20170129" //VERIFIED
	    billperiod = "T20170201"     //VERIFIED

	var start_time = time.Now().Format("2006-01-02 15:04:05")
	fmt.Print("Start time: ")
	fmt.Println(start_time)
	fmt.Println(billperiod)
	errorF := CreateGZ(OUTPATH+"logs/log"+billperiod+"_"+billcycle+"errors.txt")
	_ = os.Mkdir(OUTPATH+"/"+billperiod, 0777)
	//  dir := os.Args[1]
	file, err := os.OpenFile(OUTPATH+"logs/log"+billperiod+"_"+billcycle+"log.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		log.Println("Failed to open log file")
	}

	multi := io.MultiWriter(file, os.Stdout)

	log.SetOutput(multi)
	//      Populate fileMap
	err = addfilelist(getFile(UBPATH + "LAYOUTS/headers.txt"))
	check(err)
	err = addrefkeylist(getFile(UBPATH + "LAYOUTS/refkeyflds.txt"))
	check(err)
	err = addoutputflds(getFile(UBPATH + "LAYOUTS/refoutputflds.txt"))
	check(err)
	err = addsummaryfiles(getFile(UBPATH + "LAYOUTS/summaryfiles.txt"))
	check(err)
	err = updateFileMap()
	check(err)

	// pathMap used to store the full path to all files for bill period
	pathMap := make(map[string]string)
	//  Get bill period level file paths - these subcycles should be first on the list
	ubFileName := "PRODTYPE"
	dir := UBPATH + "ubbase/MAF_" + ubFileName
	eBillPermissionsFile := UBPATH + "ubbase" + "/USER_HIERARCHIES_REP.txt"
	//load permission map
	var permission_map = make(map[string]string)
	log.Println("Load User Permissions")
	err = makePermissionMap(getFile(eBillPermissionsFile), permission_map)
	err = filepath.Walk(dir, getSubCycles(billperiod))
	if err != nil {
		log.Fatal(err)
	}
	// Get path for data cycle files
	ubFileName = "CUSTHIER"
	dir = UBPATH + "ubbase/MAF_" + ubFileName
	err = filepath.Walk(dir, getSubCycles(billperiod+"_"+billcycle))
	log.Println("GetCycles for " + billperiod + "_" + billcycle)
	// Get path for data sub-cycle files
	ubFileName = "SAXREF"
	dir = UBPATH + "ubbase/MAF_" + ubFileName
	if subcycle == "" {
		log.Println("GetSubCycles for " + billperiod + "_" + billcycle)
	} else {
		log.Println("GetSubCycles for " + billperiod + "_" + billcycle + "_" + subcycle)
	}
	if subcycle == "" {
		err = filepath.Walk(dir, getSubCycles(billperiod+"_"+billcycle))
	} else {
		err = filepath.Walk(dir, getSubCycles(billperiod+"_"+billcycle+"_"+subcycle))
	}
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Get paths for all files")
	for key, _ := range fileMap {
		dir = UBPATH + "ubbase/MAF_" + key
		directories, _ := ioutil.ReadDir(dir)
		for _, f := range directories {
			if f.Name() >= billperiod[1:] && f.Name() != "old" {
				subdir := dir + "/" + f.Name()
				err = filepath.Walk(subdir, getFilePaths(billperiod, billcycle, subcycle, key, pathMap))
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	}
	// Load reference file maps
	var transcode_map = make(map[string]string)
	var chrggrp_map = make(map[string]string)
	var prodfam_map = make(map[string]string)
	var prodtype_map = make(map[string]string)
	var provider_map = make(map[string]string)
	var svcprovd_map = make(map[string]string)
	// var legend_map = make(map[string]string)

	// Load reference file maps
	var aggrxref_map = make(map[string]string)
	var saxref_map = make(map[string]string)
	var aggrdesc_map = make(map[string]string)
	var address_map = make(map[string]string)
	var accountTotalMap = make(map[string]string)
	var acctaddr_map = make(map[string]string)
	var circinfo_map = make(map[string]string)
	var commit_map = make(map[string]string)

	//

	//****************************************************
	// Find all TC keys in SUBASUM
	//****************************************************
	sort.Strings(subcycles)
	for m := range subcycles {
		if val, ok := pathMap["SUBASUM_"+subcycles[m]]; ok {
			log.Println("Loading subasum for transcode_map " + subcycles[m])
			err = makesubasumTC(getFile(val), prodtype_map, transcode_map)
		}
	}
	refFiles := "AGGRXREF , SAXREF, AGGRDESC, ADDRESS, PRODTYPE, PRODFAM, CHRGGRP, TRANCODE, RTEPRDCD, SVCPROVD, ISOCNTRY, MBLEGEND, PROVIDER, CIRCINFO, SERIALNM, LEGENDS, STCNTRY, CMTPRDCD, DVSNCD, DISCINFO, MRATECD, FTRCD, UOMCD, GDCFILE, GDRFILE, ITEMTP, MRTPRDCD, MBLEGEND"
	bpFiles := "PRODTYPE, PRODFAM, CHRGGRP, TRANCODE, RTEPRDCD, MBLEGEND, LEGENDS, STCNTRY, CMTPRDCD, SVCPROVD, ISOCNTRY, DVSNCD, DISCINFO, MRATECD, FTRCD, UOMCD, GDCFILE, GDRFILE, ITEMTP, MRTPRDCD, MBLEGE"
	cycleFiles := "CUSTHIER, IBSEFILE, IBSHIER, PROVIDER, VOEXGUSG, COMMIT, CIRCINFO, SERIALNM"

	//  Check for missing files - skip the cycle level files
	fatalerror := 0
	for m := range subcycles {
		log.Println("Checking files for subcycle: " + subcycles[m])
		for key, val := range fileMap {
			idxbp := strings.Index(bpFiles, key)
			idxcf := strings.Index(cycleFiles, key)
			idxcycle := strings.Index(subcycles[m], "_C00")
			idxsubcycle := strings.Index(subcycles[m], "_SC00")
			if _, ok := pathMap[key+"_"+subcycles[m]]; ok {
				//	log.Println("Processing File:"+val)
			} else {
				if val.summaryfile {
					//summary files are only output files.
				} else {
					if idxbp == -1 && idxcf == -1 && idxcycle == -1 && idxsubcycle == -1 {
						log.Println("Error - Missing Detail File:" + key)
						fatalerror++
					} else {
						if idxbp != -1 && idxcycle != -1 { //Missing Ref Files
							log.Println("Error - Missing Bill Period File:" + key)
							fatalerror++
						} else {
							if idxcf != -1 && idxcycle == -1 && idxsubcycle != -1 { //Missing Ref Files
								log.Println("Error - Missing Cycle File:" + key)
								fatalerror++
							}
						}
					}
				}
			}
		}
	}
	//
	if fatalerror > 0 {
		os.Exit(1)
	}
	for m := range subcycles {
		log.Println("Processing subcycle: " + subcycles[m])
		// Reference file load - the following will only be true for the C00 cycle
		if val, ok := pathMap["PRODFAM"+"_"+subcycles[m]]; ok {
			log.Println("Loading PRODFAM map")
			err = loadMap(getFile(val), "PRODFAM", prodfam_map)
			check(err)
		}
		if val, ok := pathMap["PRODTYPE"+"_"+subcycles[m]]; ok {
			log.Println("Loading PRODTYPE map")
			err = loadMap(getFile(val), "PRODTYPE", prodtype_map)
			check(err)
		}
		if val, ok := pathMap["CHRGGRP"+"_"+subcycles[m]]; ok {
			log.Println("Loading CHRGGRP map")
			err = loadMap(getFile(val), "CHRGGRP", chrggrp_map)
			check(err)
		}
		if val, ok := pathMap["TRANCODE"+"_"+subcycles[m]]; ok {
			log.Println("Loading TRANCODE map")
			err = loadMap(getFile(val), "TRANCODE", transcode_map)
			check(err)
		}
		if val, ok := pathMap["PROVIDER"+"_"+subcycles[m]]; ok {
			log.Println("Loading PROVIDER map")
			//			err = providerMap(getFile(val), provider_map)
			err = loadMap(getFile(val), "PROVIDER", provider_map)
			check(err)
		}
		if val, ok := pathMap["SVCPROVD"+"_"+subcycles[m]]; ok {
			log.Println("Loading SVCPROVD map")
			//		err = svcprovdMap(getFile(val), svcprovd_map)
			err = loadMap(getFile(val), "SVCPROVD", svcprovd_map)
			check(err)
		}
		// end reference file load section

		// delete all map entries and clear arrays from previous subcycles
		for mkey, _ := range aggrxref_map {
			delete(aggrxref_map, mkey)
		}
		for mkey, _ := range aggrdesc_map {
			delete(aggrdesc_map, mkey)
		}
		for mkey, _ := range circinfo_map {
			delete(circinfo_map, mkey)
		}
		for mkey, _ := range acctaddr_map {
			delete(acctaddr_map, mkey)
		}
		for mkey, _ := range address_map {
			delete(address_map, mkey)
		}
		for mkey, _ := range saxref_map {
			delete(saxref_map, mkey)
		}
		for mkey, _ := range accountTotalMap {
			delete(accountTotalMap, mkey)
		}
		// Load xref files - this will be for all non-C00 cycles

		if val, ok := pathMap["AGGRDESC"+"_"+subcycles[m]]; ok {
			log.Println("Loading AGGRDESC map: ")
			//		err = aggrdescMap(getFile(val), aggrdesc_map)
			err = loadMap(getFile(val), "AGGRDESC", aggrdesc_map)
			check(err)
		}
		if val, ok := pathMap["ADDRESS"+"_"+subcycles[m]]; ok {
			log.Println("Loading ADDRESS map: ")
			err = loadMap(getFile(val), "ADDRESS", address_map)
			check(err)
		}
		if val, ok := pathMap["AGGRXREF"+"_"+subcycles[m]]; ok {
			log.Println("Loading AGGRXREF map: ")
			err = loadMap(getFile(val), "AGGRXREF", aggrxref_map)
			check(err)
		}
		if val, ok := pathMap["SAXREF"+"_"+subcycles[m]]; ok {
			log.Println("Loading SAXREF map: ")
			err = loadMap(getFile(val), "SAXREF", saxref_map)
			check(err)
		}
		if val, ok := pathMap["ACCTADDR"+"_"+subcycles[m]]; ok {
			log.Println("Loading ACCTADDR map: ")
			err = loadMap(getFile(val), "ACCTADDR", acctaddr_map)
			check(err)
		}
		if val, ok := pathMap["CIRCINFO"+"_"+subcycles[m]]; ok {
			log.Println("Loading CIRCINFO map: ")
			err = loadMap(getFile(val), "CIRCINFO", circinfo_map)
			check(err)
		}
		if val, ok := pathMap["COMMIT"+"_"+subcycles[m]]; ok {
			log.Println("Loading COMMIT map: ")
			for mkey, _ := range commit_map {
				delete(commit_map, mkey)
			}
			err = commitMap(getFile(val), commit_map, permission_map)
			check(err)
		}
		for key, _ := range fileMap {
			if val, ok := pathMap[key+"_"+subcycles[m]]; ok {
				idx := strings.Index(refFiles, key)
				if idx == -1 { //skip files in the reference file list.
					log.Println("Details:" + key + ":")
					err = detailFile(getFile(val), subcycles[m], key, accountTotalMap, aggrxref_map, saxref_map, prodfam_map, prodtype_map, chrggrp_map, transcode_map, address_map, aggrdesc_map, svcprovd_map, provider_map, permission_map, commit_map,errorF)
					check(err)
				}

			}
		}
	}
	CloseGZ(errorF)
	var end_time = time.Now().Format("2006-01-02 15:04:05")
	log.Println("Start:" + start_time)
	log.Println("End:" + end_time)
}

// ************************ END of main ***************************************
//****************************************************************************
// detailFile - Process each detail file
//****************************************************************************
func detailFile(detFileIn <-chan string, subcycle, filebase string, accountTotalMap, aggrxref_map, saxref_map, prodfam_map, prodtype_map, chrggrp_map, transcode_map, address_map, aggrdesc_map, svcprovd_map, provider_map, permission_map, commit_map map[string]string, errorF F) error {
	line := ""
	done_ub := false
	count := 0
	var summary_map = make(map[string]string)

	outF := CreateGZ(OUTPATH + "/" + billperiod + "/" + subcycle + "_" + filebase + "_DETAILS.CSV.gz")

	//determine fields
	fileinfo := fileMap[filebase]
	var sr []string
	if fileMap[filebase].summaryfiles != "" {
		sr = strings.Split(fileMap[filebase].summaryfiles, "|")
	}
	outputs := strings.Split(fileinfo.outputfields, "|")
	svcarrtxt := ""
	aggrtxt := ""
	pftxt := ""
	pttxt := ""
	cgtxt := ""
	tctxt := ""
	addrtxt := ""
	svcprvdtxt := ""
	aggdesctxt := ""
	validusrtxt := ""

	svcarrhdrtxt := ""
	aggrhdrtxt := ""
	pfhdrtxt := ""
	pthdrtxt := ""
	cghdrtxt := ""
	tchdrtxt := ""
	addrhdrtxt := ""
	svcprvdhdrtxt := ""
	aggdeschdrtxt := ""
	validusrhdrtxt := ""

	if fileinfo.invnumidx != -1 {
		validusrhdrtxt = "|ACCT_ID|VALIDUSERS"
	}
	if fileinfo.aggridx != -1 {
		aggrhdrtxt = "|" + aggrxref_map["HEADER|"]
	}
	if fileinfo.pfidx != -1 {
		pfhdrtxt = "|" + prodfam_map["HEADER|"]
	}
	if fileinfo.ptidx != -1 {
		pthdrtxt = "|" + prodtype_map["HEADER|"]
	}
	if fileinfo.cgidx != -1 {
		cghdrtxt = "|" + chrggrp_map["HEADER|"]
	}
	if fileinfo.tcidx != -1 {
		tchdrtxt = "|" + transcode_map["HEADER|"]
	}
	if fileinfo.svcarridx != -1 {
		svcarrhdrtxt = "|" + saxref_map["HEADER|"]
		aggrhdrtxt = "|" + aggrxref_map["HEADER|"]
		addrhdrtxt = "|" + address_map["HEADER|"]
		svcprvdhdrtxt = "|" + provider_map["HEADER|"]
		idx := strings.Index(aggrdesc_map["HEADER|"], "|")
		if idx == -1 {
			idx = 0
		}
		aggdeschdrtxt = "|ACCOUNT_CUST_LABEL|GROUP_CUST_LABEL|SUBA_CUST_LABEL|AGG_CUST_LABEL|SVCARRID_CUST_LABEL" + aggrdesc_map["HEADER|"][idx:]
		validusrhdrtxt = "|ACCT_ID|VALIDUSERS"
	}
	if fileinfo.svcprdidx != -1 {
		svcprvdhdrtxt = "|" + provider_map["HEADER|"]
	}

	fullhdr := fileinfo.outputfields + pfhdrtxt + pthdrtxt + cghdrtxt + tchdrtxt + aggrhdrtxt + svcarrhdrtxt + svcprvdhdrtxt + addrhdrtxt + aggdeschdrtxt + validusrhdrtxt

	lastErrSvcId := ""
	lastSvcID := ""
	lastpf := ""
	lastpt := ""
	lastcg := ""
	lasttc := ""
	for done_ub == false {
		line = <-detFileIn
		if line == "EOF" {
			done_ub = true
		} else {
			w := strings.Split(line, "|")
			if len(w) > 2 {
				templine := ""
				for i := 0; i < len(outputs); i++ {
					if fileinfo.keepflds[i] < len(w) {
						if fileinfo.keepflds[i] == -1 {
							templine = templine + "|"
						} else {
							templine = templine + "|" + strings.TrimSpace(w[fileinfo.keepflds[i]])
						}
					} else {
						templine = templine + "|"
					}
				}
				if len(templine) > 0 {
					templine = templine[1:]
				}
				if fileinfo.invnumidx > -1 {
					if val, OK := commit_map[strings.TrimSpace(w[fileinfo.invnumidx])]; OK {
						a := strings.Split(val, "|")
						validusrtxt = "|" + a[0]+ "|" + a[4]  //ACCT_ID & users
					} else {
						fmt.Println("ERROR: missing INVOICE_NUM ON COMMIT: " + strings.TrimSpace(w[fileinfo.invnumidx]))
					}
				}
				if fileinfo.svcarridx > 0 {
					if strings.TrimSpace(w[fileinfo.svcarridx]) != lastSvcID {
						lastSvcID = strings.TrimSpace(w[fileinfo.svcarridx])
						if val, OK := saxref_map[strings.TrimSpace(w[fileinfo.svcarridx])]; OK {
							svcarrtxt = "|SAXREF FIELDS***" + strconv.Itoa(fileinfo.svcarridx) + "****" + val + "****END SAXREF***"
						} else {
							svcarrtxt = "|SAXREF FIELDS***" + strconv.Itoa(fileinfo.svcarridx) + "****" + saxref_map[strings.TrimSpace("BLANK|")] + "****END SAXREF***"
						}
						sf := strings.Split(saxref_map[w[fileinfo.svcarridx]], "|")
						if len(sf) > 7 {
							aggridx := 7        //hard coded to improve performance
							saxrefProvidx := 15 //hard coded to improve performance
							if val, OK := aggrxref_map[strings.TrimSpace(sf[aggridx])]; OK {
								aggrtxt = "|AGGRXREF FIELDS***" + val + "****END AGGRXREF***"
							} else {
								aggrtxt = "|AGGRXREF BLANK FIELDS***" + aggrxref_map[strings.TrimSpace("BLANK|")] + "****END AGGRXREF***"
							}
							addrtxt = "|ADDRESS FIELDS***" + addrFields(w[fileinfo.svcarridx], aggrxref_map[sf[aggridx]], address_map) + "****END ADDRESS***"
							a := strings.Split(saxref_map[strings.TrimSpace(w[fileinfo.svcarridx])], "|")
							//a[1] is the aggregator level ID
							aggdesctxt = "|AGGDESC FIELDS***" + aggdescFields(w[fileinfo.svcarridx], a[1], aggrxref_map[sf[aggridx]], aggrdesc_map) + "****END AGGDESC***"
							aggrxrefflds := strings.Split(aggrxref_map[sf[aggridx]], "|")
							if len(aggrxrefflds) > 6 {
								// Add Group level permissions
								if len(aggrxrefflds[1]) > 2 {
									key := aggrxrefflds[2][2:] + aggrxrefflds[6] + "-" // GROUPFMT IS [6]
									if txt, OK := permission_map[key]; OK {
										validusrtxt = validusrtxt + "," + txt
										//				fmt.Println("Group Level Permisions:" + txt)
									} else {
										//    		fmt.Println("No users for:"+key)
									}
								}
								// Add Subaccount level permissions
								if len(aggrxrefflds[0]) > 2 {
									key := aggrxrefflds[2][2:] + aggrxrefflds[6] + aggrxrefflds[0][2:]
									if txt, OK := permission_map[key]; OK {
										validusrtxt = validusrtxt + "," + txt
										//					fmt.Println("Subaccount Level Permisions:" + txt)
									} else {
										//    		fmt.Println("No users for:"+key)
									}
								}
							}
							if len(sf) > 15 {
								if val, OK := provider_map[strings.TrimSpace(sf[saxrefProvidx])]; OK {
									svcprvdtxt = "|SAXREF PROVIDER FIELDS***" + val + "****PROVIDER***"
								} else {
									svcprvdtxt = "|PROVIDER FIELDS***" + provider_map[strings.TrimSpace("BLANK|")] + "****PROVIDER***"
								}
							}

						} else {
							if w[fileinfo.svcarridx] != lastErrSvcId {
								svcarrtxt = "|SAXREF BLANK***" + strconv.Itoa(fileinfo.svcarridx) + "****" + saxref_map[strings.TrimSpace("BLANK|")] + "****END SAXREF***"
	WriteGZ(errorF, billperiod + "/" + subcycle + "_" + filebase+"|"+"SAXREF KEY|"+ w[fileinfo.svcarridx]+"\n")
			//					log.Println("Error SAXREF has too few fields" + w[fileinfo.svcarridx])
								lastErrSvcId = w[fileinfo.svcarridx]
							}
						}
					}
				} else {
					if fileinfo.aggridx != -1 {
						if val, OK := aggrxref_map[w[fileinfo.aggridx]]; OK {
							aggrtxt = "|AGGRXREF FIELDS***" + val + "****END AGGRXREF***"
						} else {
							aggrtxt = "|AGGRXREF BLANK FIELDS***" + aggrxref_map[strings.TrimSpace("BLANK|")] + "****END AGGRXREF***"
						}
					}
				}

				if fileinfo.pfidx > 0 {
					if lastpf != strings.TrimSpace(w[fileinfo.pfidx]) {
						lastpf = strings.TrimSpace(w[fileinfo.pfidx])

						if val, OK := prodfam_map[strings.TrimSpace(w[fileinfo.pfidx])]; OK {
							pftxt = "|PF FIELDS***" + val + "****PF***"
						} else {
							pftxt = "|PF FIELDS***" + prodfam_map[strings.TrimSpace("BLANK|")] + "****PF***"
						}
					}
				}
				if fileinfo.ptidx > 0 {
					if lastpt != strings.TrimSpace(w[fileinfo.ptidx]) {
						lastpt = strings.TrimSpace(w[fileinfo.ptidx])
						if val, OK := prodtype_map[strings.TrimSpace(w[fileinfo.ptidx])]; OK {
							pttxt = "|PT FIELDS***" + val + "****PT***"
						} else {
							pttxt = "|PT FIELDS***" + prodtype_map[strings.TrimSpace("BLANK|")] + "****PT***"
						}
					}
				}
				if fileinfo.cgidx > 0 {
					if lastcg != strings.TrimSpace(w[fileinfo.cgidx]) {
						lastcg = strings.TrimSpace(w[fileinfo.cgidx])
						if val, OK := chrggrp_map[strings.TrimSpace(w[fileinfo.cgidx])]; OK {
							cgtxt = "|CG FIELDS***" + val + "****CG***"
						} else {
							cgtxt = "|CG FIELDS***" + chrggrp_map[strings.TrimSpace("BLANK|")] + "****CG***"
						}
					}
				}
				if fileinfo.tcidx > 0 {
					if lasttc != strings.TrimSpace(w[fileinfo.tcidx]) {
						lasttc = strings.TrimSpace(w[fileinfo.tcidx])
						if val, OK := transcode_map[strings.TrimSpace(w[fileinfo.tcidx])]; OK {
							tctxt = "|TC FIELDS***" + val + "****TC***"
						} else {
							tctxt = "|TC FIELDS***" + transcode_map[strings.TrimSpace("BLANK|")] + "****TC***"
						}
					}
				}

				if fileinfo.svcprdidx > 0 {
					if val, OK := provider_map[strings.TrimSpace(w[fileinfo.svcprdidx])]; OK {
						svcprvdtxt = "|DETAIL PROVIDER FIELDS***" + val + "****PROVIDER***"
					}
				}

				if count < MAXDET {
					if count < 1 {
						WriteGZ(outF, fullhdr+"\n")
					}
					fulldtl := templine + pftxt + pttxt + cgtxt + tctxt + aggrtxt + svcarrtxt + svcprvdtxt + addrtxt + aggdesctxt + validusrtxt
					WriteGZ(outF, fulldtl+"\n")

					for j := 0; j < len(sr); j++ {
						sumrec := fileMap[sr[j]]
						sumhdr := ""
						sumdtl := ""
						if sumrec.pfidx != -1 {
							sumhdr = sumhdr + pfhdrtxt
							sumdtl = sumdtl + pftxt
						}
						if sumrec.ptidx != -1 {
							sumhdr = sumhdr + pthdrtxt
							sumdtl = sumdtl + pttxt
						}
						if sumrec.cgidx != -1 {
							sumhdr = sumhdr + cghdrtxt
							sumdtl = sumdtl + cgtxt
						}
						if sumrec.tcidx != -1 {
							sumhdr = sumhdr + tchdrtxt
							sumdtl = sumdtl + tctxt
						}
						if sumrec.aggridx != -1 {
							sumhdr = sumhdr + aggrhdrtxt
							sumdtl = sumdtl + aggrtxt
						}
						if sumrec.svcarridx != -1 {
							sumhdr = sumhdr + aggrhdrtxt + svcarrhdrtxt + svcprvdhdrtxt + addrhdrtxt + aggdeschdrtxt
							sumdtl = sumdtl + aggrtxt + svcarrtxt + svcprvdtxt + addrtxt + aggdesctxt
						}
						sumhdr = sumhdr + validusrhdrtxt
						sumdtl = sumdtl + validusrtxt
						sumrec.sumhdr = sumhdr
						fileMap[sr[j]] = sumrec
						err := summaryFile(filebase, sr[j], sumrec.keys, sumhdr, sumdtl, line, sumrec.keepflds, summary_map)
						check(err)
					}

				} else {
					done_ub = true // tempoary code to speed up testing.
				}
				count++
			}
		}
	}
	//after reads
	log.Println(filebase + " Count:" + strconv.Itoa(count))
	CloseGZ(outF)
	count = 0
	for j := 0; j < len(sr); j++ {
		sumrec := fileMap[sr[j]]
		outF := CreateGZ(OUTPATH + "/" + billperiod + "/" + subcycle + "_" + sr[j] + "_SUMMARY.CSV.gz")
		summaryhdr := sumrec.outputfields + sumrec.sumhdr
		WriteGZ(outF, summaryhdr+"\n")
		for key, val := range summary_map {
			keys := strings.Split(key, "~")
			if keys[0] == sr[j] {
				WriteGZ(outF, val+"\n")
				count++
			}
		}
		log.Println(sr[j] + " Count:" + strconv.Itoa(count))
		CloseGZ(outF)
	}
	return nil
}

func summaryFile(filebase, sumbase, keyflds, derivedhdr, deriveddtl, detail string, keepflds [500]int, summary_map map[string]string) error {
  sumrec := fileMap[sumbase]
	oflds := strings.Split(sumrec.outputfields, "|")
//	f := strings.Split(sumrec.fields, "|")
	kflds := strings.Split(keyflds, "|")
	dd := strings.Split(detail, "|")

	key := sumbase + "~"
	for k := 0; k < len(kflds); k++ {
		for i := 0; i < len(oflds); i++ {
			if oflds[i] == kflds[k] {
				if len(dd) > i {
					key = key + dd[keepflds[i]]
				}
			}
		}
	}
	//000000001|000000000.620000|000000000.00|000000000.00
	numflds := "COMPLETED|PREDSCCHG|DSCAMOUNT|TAXAMOUNT|CALLDURATN"
	if val, OK := summary_map[key]; OK {
		sumflds := strings.Split(val, "|")
		for x := 0; x < len(oflds); x++ {
			if keepflds[x] == -1 {
				sumflds[x] = ""
			} else {
				if strings.Index(numflds, oflds[x]) != -1 {
					tmpflt1, _ := strconv.ParseFloat(dd[keepflds[x]], 64)
					tmpflt2, _ := strconv.ParseFloat(sumflds[x], 64)
					tmpflt3 := tmpflt1 + tmpflt2
					sumflds[x] = strconv.FormatFloat(tmpflt3, 'f', 2, 64)
				} else {
					sumflds[x] = dd[keepflds[x]]
				}
			}
		}
		summary_map[key] = sumflds[0]
		for x := 1; x < len(oflds); x++ {
			summary_map[key] = summary_map[key] + "|" + sumflds[x]
		}
	} else {
		if keepflds[0] == -1 {
			summary_map[key] = ""
		} else {
			summary_map[key] = dd[keepflds[0]]
		}
		for x := 1; x < len(oflds); x++ {
			if keepflds[x] == -1 {
				summary_map[key] = summary_map[key] + "|"
			} else {
				summary_map[key] = summary_map[key] + "|" + dd[keepflds[x]]
			}
		}
	}
	summary_map[key] = summary_map[key] + deriveddtl
	return nil
}
func aggdescFields(serviceid, agglvlid, aggrflds string, aggrdesc_map map[string]string) string {
	// the following fields are added to the header |ACCOUNT_CUST_LABEL|GROUP_CUST_LABEL|SUBA_CUST_LABEL|AGG_CUST_LABEL|SERV_CUST_LABEL|
	tmpout := ""
	var a []string
	var g []string
	var sa []string
	var svc []string
	var agg []string
	acctlbl := ""
	grouplbl := ""
	subalbl := ""
	agglbl := ""
	svcidlbl := ""
	out := strings.Split(aggrdesc_map["BLANK|"], "|")
	blanks := strings.Split(aggrdesc_map["BLANK|"], "|")
	acctkey := ""
	groupkey := ""
	subaccountkey := ""

	w := strings.Split(aggrflds, "|")
	if len(w) >= 4 {

		//			acctkey = "13" + w[2][2:]
		//			groupkey = "14" + w[1][2:]
		//			subaccountkey = "15" + w[0][2:]
		acctkey = w[2]
		groupkey = w[1]
		subaccountkey = w[0]
		if val, OK := aggrdesc_map[strings.TrimSpace(serviceid)]; OK {
			svc = strings.Split(val, "|")
		} else {
			svc = strings.Split("", "|")
		}
		if len(svc) > 2 {
			svcidlbl = svc[0]
			for i := 0; i < len(svc); i++ {
				if len(out) >= i {
					if out[i] == blanks[i] {
						out[i] = svc[i] // sa value may also be null
					}
				}
			}
		}
		if val, OK := aggrdesc_map[strings.TrimSpace(agglvlid)]; OK {
			agg = strings.Split(val, "|")
		} else {
			agg = strings.Split("", "|")
		}
		if len(svc) > 2 {
			agglbl = agg[0]
			for i := 0; i < len(agg); i++ {
				if len(out) >= i {
					if out[i] == blanks[i] {
						out[i] = agg[i] // sa value may also be null
					}
				}
			}
		}
		if val, OK := aggrdesc_map[strings.TrimSpace(subaccountkey)]; OK {
			sa = strings.Split(val, "|")
		} else {
			sa = strings.Split("", "|")
		}
		if len(sa) > 2 {
			subalbl = sa[0]
			for i := 0; i < len(sa); i++ {
				if len(out) >= i {
					if out[i] == blanks[i] {
						out[i] = sa[i] // sa value may also be null
					}
				}
			}
		}
		if val, OK := aggrdesc_map[strings.TrimSpace(groupkey)]; OK {
			g = strings.Split(val, "|")
		} else {
			g = strings.Split("", "|")
		}
		if len(g) > 2 {
			grouplbl = g[0]
			for i := 0; i < len(g); i++ {
				if len(out) >= i {
					if out[i] == blanks[i] {
						out[i] = g[i] // sa value may also be null
					}
				}
			}
		}
		if val, OK := aggrdesc_map[strings.TrimSpace(acctkey)]; OK {
			a = strings.Split(val, "|")
		} else {
			a = strings.Split("", "|")
		}
		if len(a) > 2 {
			acctlbl = a[0]
			for i := 0; i < len(a); i++ {
				if len(out) >= i {
					if out[i] == blanks[i] {
						out[i] = a[i] // sa value may also be null
					}
				}
			}
		}
	} else {
		log.Println("Error in addrFields - invalid aggrflds")
		svc = strings.Split(aggrdesc_map["BLANK|"], "|")
	}
	tmpout = acctlbl + "|" + grouplbl + "|" + subalbl + "|" + agglbl + "|" + svcidlbl
	if len(out) > 2 {
		//		tmpout = out[0]
		for i := 1; i < len(out); i++ {
			tmpout = tmpout + "|" + out[i]
		}
	}
	return tmpout
}
func addrFields(serviceid, aggrflds string, address_map map[string]string) string {

	tmpout := ""
	var a []string
	var g []string
	var sa []string
	var svc []string
	out := strings.Split(address_map["BLANK|"], "|")
	blanks := strings.Split(address_map["BLANK|"], "|")

	w := strings.Split(aggrflds, "|")
	if len(w) >= 4 {
		acctkey := w[2]
		groupkey := w[1]
		subaccountkey := w[0]
		if val, OK := address_map[strings.TrimSpace(serviceid)]; OK {
			svc = strings.Split(val, "|")
		} else {
			svc = strings.Split("", "|")
		}
		if len(svc) > 2 {
			for i := 0; i < len(svc); i++ {
				if len(out) >= i {
					if out[i] == blanks[i] {
						out[i] = svc[i] // sa value may also be null
					}
				}
			}
		}
		if val, OK := address_map[strings.TrimSpace(subaccountkey)]; OK {
			sa = strings.Split(val, "|")
		} else {
			sa = strings.Split("", "|")
		}
		if len(sa) > 2 {
			for i := 0; i < len(sa); i++ {
				if len(out) >= i {
					if out[i] == blanks[i] {
						out[i] = sa[i] // sa value may also be null
					}
				}
			}
		}
		if val, OK := address_map[strings.TrimSpace(groupkey)]; OK {
			g = strings.Split(val, "|")
		} else {
			g = strings.Split("", "|")
		}
		if len(g) > 2 {
			for i := 0; i < len(g); i++ {
				if len(out) >= i {
					if out[i] == blanks[i] {
						out[i] = g[i] // sa value may also be null
					}
				}
			}
		}
		if val, OK := address_map[strings.TrimSpace(acctkey)]; OK {
			a = strings.Split(val, "|")
		} else {
			a = strings.Split("", "|")
		}
		if len(a) > 2 {
			for i := 0; i < len(a); i++ {
				if len(out) >= i {
					if out[i] == blanks[i] {
						out[i] = a[i] // sa value may also be null
					}
				}
			}
		}
	} else {
		log.Println("Error in addrFields - invalid aggrflds")
		svc = strings.Split(address_map["BLANK|"], "|")
	}
	if len(out) > 2 {
		tmpout = out[0]
		for i := 1; i < len(out); i++ {
			tmpout = tmpout + "|" + out[i]
		}
	}
	return tmpout
}

func getSubCycles(searchparm string) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Print(err)
			return nil
		}
		idx := strings.Index(path, searchparm)
		idx2 := strings.Index(path, ".DAT")
		if (idx != -1) && (idx2 != -1) {
			subcycles = append(subcycles, path[idx:idx+18])
		}
		return nil
	}
}

func getFilePaths(billperiod, billcycle, subcycle, ubFileName string, pathMap map[string]string) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Print(err)
			return nil
		}
		subcyclestr := ""
		cyclestring := billcycle
		if subcycle != "" {
			cyclestring = billcycle + "_" + subcycle
		}
		if ubFileName == "TRANCODE" {
			idx := strings.Index(path, "SUB_FTRANCODE_ID1_")
			if idx != -1 {
				idx = idx + 18 // should map to same place as bill period.
				subcyclestr = billperiod + "_C00_SC00"
				if pathMap[ubFileName+"_"+subcyclestr] == "" {
					pathMap[ubFileName+"_"+subcyclestr] = path
					log.Println("TRANCODE:" + path)
				}
			}
		} else {
			idx := strings.Index(path, billperiod)
			idx1 := strings.Index(path, billperiod+"_"+cyclestring)
			idx2 := strings.Index(path, billperiod+"_C00")
			dat := strings.Index(path, ".DAT")
			// May need to check for DAT vs UTF file names -  for now it is taking UTF since it is second.
			if dat != -1 && idx != -1 && (idx1 != -1 || idx2 != -1) {
				pathMap[ubFileName+"_"+path[idx:idx+18]] = path
			}
		}
		return nil
	}
}

//****************************************************************************
// addfilelist - initialize FileInfo
//****************************************************************************
func addfilelist(file <-chan string) error {
	line := ""
	done_ub := false
	filename := ""
	var fileinfo FileInfo
	for done_ub == false {
		line = <-file
		if line == "EOF" {
			done_ub = true
		} else {
			i := strings.Index(line, "|")
			if i != -1 {
				filename = strings.TrimSpace(line[:i])
				fileinfo.fields = strings.TrimSpace(line[i+1:])
				fileinfo.keys = ""
				fileinfo.outputfields = strings.TrimSpace(line[i+1:])
				fileinfo.reffile = false
				fileinfo.bpfile = false
				fileinfo.cyclefile = false
				fileinfo.subcyclefile = false
				fileinfo.summaryfile = false
				fileinfo.summaryfiles = ""
				fileinfo.aggridx = -1
				fileinfo.svcarridx = -1
				fileinfo.pfidx = -1
				fileinfo.ptidx = -1
				fileinfo.cgidx = -1
				fileinfo.tcidx = -1
				fileinfo.svcprdidx = -1
				fileinfo.invnumidx = -1
				fileMap[filename] = fileinfo

			}
		}
	}
	return nil
}

//****************************************************************************
// addrefkeylist- identify ref files and add the keylist
//****************************************************************************
func addrefkeylist(file <-chan string) error {
	line := ""
	var fileinfo FileInfo
	done_ub := false
	for done_ub == false {
		line = <-file
		if line == "EOF" {
			done_ub = true
		} else {
			i := strings.Index(line, "|")
			if i != -1 {
				filename := strings.TrimSpace(line[:i])
				if val, ok := fileMap[filename]; ok {
					val.keys = strings.TrimSpace(line[i+1:])
					fileMap[filename] = val
				} else {
					fileinfo.fields = "" //should be a summary file
					fileinfo.keys = strings.TrimSpace(line[i+1:])
					fileinfo.outputfields = ""
					fileinfo.reffile = false
					fileinfo.bpfile = false
					fileinfo.cyclefile = false
					fileinfo.subcyclefile = false
					fileinfo.summaryfile = true
					fileinfo.summaryfiles = ""
					fileMap[filename] = fileinfo
				}
			}
		}
	}
	return nil
}

//****************************************************************************
// addoutputflds- identify to be output
//****************************************************************************
func addoutputflds(file <-chan string) error {
	line := ""
	done_ub := false

	for done_ub == false {
		line = <-file
		if line == "EOF" {
			done_ub = true
		} else {
			i := strings.Index(line, "|")
			if i != -1 {
				filename := strings.TrimSpace(line[:i])
				if val, ok := fileMap[filename]; ok {
					val.outputfields = strings.TrimSpace(line[i+1:])
					fileMap[filename] = val
				}
			}
		}
	}
	return nil
}

//****************************************************************************
// addoutputflds- identify to be output
//****************************************************************************
func addsummaryfiles(file <-chan string) error {
	line := ""
	done_ub := false

	for done_ub == false {
		line = <-file
		if line == "EOF" {
			done_ub = true
		} else {
			i := strings.Index(line, "|")
			if i != -1 {
				filename := strings.TrimSpace(line[:i])
				if val, ok := fileMap[filename]; ok {
					val.summaryfiles = strings.TrimSpace(line[i+1:])
					fileMap[filename] = val
				}
			}
		}
	}
	return nil
}

//****************************************************************************
// updateFileMap-
//****************************************************************************
func updateFileMap() error {
	log.Println("Set all file indicators")
	for _, val := range fileMap {
		if val.summaryfiles != "" {

			sf := strings.Split(val.summaryfiles, "|")
			for i := 0; i < len(sf); i++ {
				//				fmt.Println(sf[i])
				suminfo := fileMap[sf[i]]
				suminfo.fields = val.fields
				suminfo.reffile = false
				suminfo.bpfile = false
				suminfo.cyclefile = false
				suminfo.subcyclefile = false
				suminfo.summaryfile = true
				suminfo.aggridx = -1
				suminfo.svcarridx = -1
				suminfo.pfidx = -1
				suminfo.ptidx = -1
				suminfo.cgidx = -1
				suminfo.tcidx = -1
				suminfo.svcprdidx = -1
				suminfo.invnumidx = -1
				fileMap[sf[i]] = suminfo
			}
		}
	}
	for key, val := range fileMap {
		f := strings.Split(val.fields, "|")
		outputs := strings.Split(val.outputfields, "|")
		for i := 0; i < len(outputs); i++ {
			val.keepflds[i] = -1
			for o := 0; o < len(f); o++ {
				if strings.TrimSpace(outputs[i]) == strings.TrimSpace(f[o]) {
					val.keepflds[i] = o

					if strings.TrimSpace(f[o]) == "INVOICE_NUM" {
						val.invnumidx = o
					}
					if strings.TrimSpace(f[o]) == "HPIDFK" {
						val.aggridx = o
					}
					if strings.TrimSpace(f[o]) == "PFAMFK" {
						val.pfidx = o
					}
					if strings.TrimSpace(f[o]) == "PTYPFK" {
						val.ptidx = o
					}
					if strings.TrimSpace(f[o]) == "CHGGFK" {
						val.cgidx = o
					}
					if strings.TrimSpace(f[o]) == "TRANFK" {
						val.tcidx = o
					}
					if strings.TrimSpace(f[o]) == "SVCARRID" {
						val.svcarridx = o
					}
					if strings.TrimSpace(f[o]) == "TEL_PROVIDER_CD" {
						val.svcprdidx = o
					}
				}
			}
					}
			fileMap[key] = val
	}
	return nil
}

//****************************************************************************
// commitMap - map COMMIT FILE records
//****************************************************************************
func commitMap(ubfile <-chan string, commit_map, permission_map map[string]string) error {
	line := ""
	done_ub := false
	key := ""
	billdt := ""
	permkey := ""
	for done_ub == false {
		line = <-ubfile
		if line == "EOF" {
			done_ub = true
		} else {
			// this file is not delimited - the fields are fixed length
			//		w := strings.Split(line, "|")
			// 00000420447587047346880741       0201702221N4C5090175            102017047000000118.26N
			if line[1:7] == "UEBILL" {
				billdt = line[38:46]
			} else {
				key = strings.TrimSpace(line[45:65])

				// ACCNUM|BILLDT|BILLPRD|BILLCY|
				permkey = strings.TrimSpace(line[13:33]) + "--"
				if txt, OK := permission_map[permkey]; OK {
					commit_map[key] = line[13:33] + "|" + billdt + "|" + line[41:43] + "|" + line[43:45] + "|" + txt
				} else {
					commit_map[key] = line[13:33] + "|" + billdt + "|" + line[41:43] + "|" + line[43:45] + "|"
				}
			}
		} //after reads
	}
	return nil
}

//****************************************************************************
// makePermissionMap - map user level permissions
//****************************************************************************
func makePermissionMap(ubfile <-chan string, permission_map map[string]string) error {
	line := ""
	done_ub := false
	key := ""
	for done_ub == false {
		line = <-ubfile
		if line == "EOF" {
			done_ub = true
		} else {
			w := strings.Split(line, "|")
			if len(w) >= 3 {
				key = strings.TrimSpace(w[1]) + strings.TrimSpace(w[2]) + strings.TrimSpace(w[3])
				if val, ok := permission_map[key]; ok {
					idx := strings.Index(val, strings.TrimSpace(w[0]))
					if idx == -1 {
						if val == "" {
							permission_map[key] = strings.TrimSpace(w[0])
						} else {
							permission_map[key] = val + "," + strings.TrimSpace(w[0])
						}
					}
				} else {
					permission_map[key] = strings.TrimSpace(w[0])
				}
			}
		} //after reads
	}
	return nil
}

//****************************************************************************
// makesubasumTC - output subasum records
//****************************************************************************
func makesubasumTC(ubfile <-chan string, prodtype_map, transcode_map map[string]string) error {
	line := ""
	done_ub := false

	for done_ub == false {
		line = <-ubfile
		if line == "EOF" {
			done_ub = true
		} else {
			w := strings.Split(line, "|")
			if len(w) >= 2 {
				if _, ok := prodtype_map[strings.TrimSpace(w[5])]; ok {
				} else {
					prodtype_map[strings.TrimSpace(w[5])] = "Y"
				}
				if _, ok := transcode_map[strings.TrimSpace(w[7])]; ok {
				} else {
					transcode_map[strings.TrimSpace(w[7])] = "Y"
				}
			}
		} //after reads
	}
	return nil
}

//****************************************************************************
// loadMap - load cross reference entries into memory map
//****************************************************************************
func loadMap(reffile <-chan string, basefile string, refmap map[string]string) error {

	fileinfo := fileMap[basefile]

	var keymap = make(map[string]bool)
	var outputmap = make(map[string]bool)
	var initfldsmap = make(map[string]bool)

	outputfields := fileinfo.outputfields
	keyfields := fileinfo.keys
	// add file for initialize fields - and perhaps some fields type process
	initvalflds := "TO_END_USR_ZIP"
	fields := fileinfo.fields
	f := strings.Split(fields, "|")
	// initialize the maps
	for i := 0; i < len(f); i++ {
		keymap[strings.TrimSpace(f[i])] = false
		outputmap[strings.TrimSpace(f[i])] = false
		initfldsmap[strings.TrimSpace(f[i])] = false
	}
	keys := strings.Split(keyfields, "|")
	for i := 0; i < len(keys); i++ {
		keymap[strings.TrimSpace(keys[i])] = true
	}
	outputs := strings.Split(outputfields, "|")
	for i := 0; i < len(outputs); i++ {
		outputmap[strings.TrimSpace(outputs[i])] = true
	}
	inits := strings.Split(initvalflds, "|")
	for i := 0; i < len(inits); i++ {
		initfldsmap[strings.TrimSpace(inits[i])] = true
	}

	templine := ""
	blankline := ""
	initval := ""
	for i := 0; i < len(f); i++ {
		//adding pipe allows no possibility of substrings from other columns matching
		if outputmap[strings.TrimSpace(f[i])] {
			initval = ""
			if initfldsmap[strings.TrimSpace(f[i])] {
				if strings.TrimSpace(f[i]) == "TO_END_USR_ZIP" {
					initval = "00000"
				} else {
					initval = ""
				}
			}
			if templine == "" {
				templine = strings.TrimSpace(f[i])
				blankline = initval
			} else {
				templine = templine + "|" + strings.TrimSpace(f[i])
				blankline = blankline + "|" + initval
			}
		}
	}
	refmap["HEADER|"] = templine
	refmap["BLANK|"] = blankline
	done_ub := false
	counter := 0
	line := ""
	templine = ""
	for done_ub == false {
		line = <-reffile
		if line == "EOF" {
			done_ub = true
		} else {
			w := strings.Split(line, "|")
			if len(w) >= 2 {
				counter++
				templine := "~start#~" //value to indicate start of record to prevent leading | - may be better to just strip the first pipe at the end.
				key := ""

				for i := 0; i < len(w); i++ {
					//adding pipe allows no possibility of substrings from other columns matching
					if i < len(f) {
						if keymap[strings.TrimSpace(f[i])] {
							if key == "" {
								key = strings.TrimSpace(w[i])
							} else {
								key = key + strings.TrimSpace(w[i])
							}
						}
						if outputmap[strings.TrimSpace(f[i])] {
							if templine == "~start#~" {
								templine = strings.TrimSpace(w[i])
							} else {
								templine = templine + "|" + strings.TrimSpace(w[i])
							}
						}
					}
				}
				if (basefile == "TRANCODE") || (basefile == "xPRODTYPE") {
					if _, ok := refmap[key]; ok {
						refmap[key] = templine
					}
				} else {
					refmap[key] = templine
				}
			}
		} //after reads
	}
	log.Println(basefile + ": " + strconv.Itoa(counter))
	return nil
}

func CreateGZ(s string) (f F) {

	fi, err := os.OpenFile(s, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	//append	fi, err := os.OpenFile(s, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		log.Println("Error in Create")
		panic(err)
	}
	gf := gzip.NewWriter(fi)
	fw := bufio.NewWriter(gf)
	f = F{fi, gf, fw}
	return
}

func WriteGZ(f F, s string) {
	(f.fw).WriteString(s)
}

func CloseGZ(f F) {
	f.fw.Flush()
	// Close the gzip first.
	f.gf.Close()
	f.f.Close()
}

func getFile(f string) <-chan string {
	out := make(chan string, 1000)
	go func() {
		theFile, err := os.Open(f)
		if err != nil {
			log.Println("Error:", err)
			return
		}
		idx := strings.Index(f, ".gz")
		if idx == -1 {
			defer theFile.Close()
			record := bufio.NewReader(theFile)
			var done bool = false
			for done == false {
				line, err := record.ReadString('\n')
				if err == nil {
					out <- line
				} else {
					out <- "EOF"
					done = true
				}
			}
		} else {
			fileGzip, err := gzip.NewReader(theFile)
			if err != nil {
				log.Println("Error:", err)
				return
			}
			fileRead := bufio.NewReader(fileGzip)
			var done bool = false
			for done == false {
				line, err := fileRead.ReadString('\n')
				if err == nil {
					out <- line
				} else {
					out <- "EOF"
					done = true
				}
			}
			fileGzip.Close()
			theFile.Close()
		}
		close(out)
	}()
	return out
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
