from hec.script import ResSim
from hec.script import Constants

from hec.heclib.dss import HecDss
from hec.io import TimeSeriesContainer
from hec.heclib.util import HecTime
from hec.hecmath import TimeSeriesMath
from hec.heclib.dss import HecDSSFileDataManager
from hec.ensemble import Ensemble, EnsembleTimeSeries
from hec import SqliteDatabase, VersionIdentifier, RecordIdentifier
from java.util import Random
from org.sqlite import JDBC
from java.time import Duration
from jarray import array
import logging
import os
import sys
import shutil
import datetime

def configureResSim(watershedWkspFile, simName, altName):

    #  Res Sim only likes unix-style path
    watershedWkspFile = watershedWkspFile.replace(os.sep, "/")
    ResSim.openWatershed(watershedWkspFile)
    ResSim.selectModule('Simulation')
    simMode = ResSim.getCurrentModule()

    # Not sure what this does, but this is the only way to open a simulation
    simMode.resetWorkspace()
    simMode.openSimulation(simName)
    simulation = simMode.getSimulation()

    # force compute everything
    simulation.setComputeAll(1)
    simRun = simulation.getSimulationRun(altName)
    return simMode, simRun

def myLogger(name, path):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(path, 'a')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def archiveSimulationResults(aepList, pattern, patternName, resultsDir, simulationDssDir, n):

    bParts = ['FOLSOM-POOL','FOLSOM-POOL','FOLSOM-POOL','FOLSOM-POOL',
              'FOLSOM-CONSERVATION','FOLSOM-CONSERVATION',
              'FOLSOM-FLOOD CONTROL','FOLSOM-FLOOD CONTROL',
              'FOLSOM-SURCHARGE','FOLSOM-SURCHARGE',
              'FOLSOM-7 ESRD SEAL 8 GATES - 120-MIN ISE SLOPE',
              'FOLSOM-EMERGENCY SPILLWAY',
              'PREDICT_PEAK_STORAGE_115',
              'PREDICT_PEAK_STORAGE_130',
              'PREDICT_PEAK_STORAGE_145',
              'PREDICT_PEAK_STORAGE_160']
    cParts = ['FLOW-IN','FLOW-OUT','ELEV','STOR',
              'ELEV-ZONE','STOR-ZONE',
              'ELEV-ZONE','STOR-ZONE',
              'ELEV-ZONE','STOR-ZONE',
              'FLOW-MIN',
              'FLOW',
              'MAX_STOR_115',
              'MAX_STOR_130',
              'MAX_STOR_145',
              'MAX_STOR_160']

    simulationDssFile = r"%s\simulation.dss" %(simulationDssDir)
    resultsDssFile = r"%s\RR-%s_results.dss" %(resultsDir, patternName)

    lookupSimulationDParts = {
        '1986': {
           datetime.datetime(1986,2,1): '01FEB1986'
        },
        '1997': {
            datetime.datetime(1996,12,1): '01DEC1996',
            datetime.datetime(1997,1,1): '01JAN1997'
        }
    }

    simulationDParts = lookupSimulationDParts[pattern]
    pathNames = []
    for bpart, cpart in zip(bParts,cParts):
        for aep in aepList:

            for dpartKey in sorted(simulationDParts.keys()):
                dpart = simulationDParts[dpartKey]
                pathName = "//%s/%s/%s/1HOUR/C:000%s|RR-%s---0/" % (bpart, cpart,dpart, aep, patternName)
                pathNames.append(pathName)

    writeResultsToFile(pathNames, simulationDssFile, resultsDssFile,n )


def writeResultsToFile(pathNames, simulationDssFile, resultsDssFile,n):

    nStr = str(n).zfill(3)
    fid = HecDss.open(simulationDssFile)
    for pathName in pathNames:

        if fid.recordExists(pathName):
            tsc = fid.get(pathName)
            if tsc.values is not None:
                parts = tsc.fullName.split('/')
                parts[6] = parts[6][:-3] + nStr
                newPathName = '/'.join(parts)

                newTs = TimeSeriesContainer()
                newTs.version = newPathName.split('/')[-2]
                newTs.fullName = newPathName
                newTs.timeGranularitySeconds = tsc.timeGranularitySeconds
                newTs.type = tsc.type
                newTs.units = tsc.units
                newTs.interval = tsc.interval
                newTs.numberValues = tsc.numberValues
                newTs.times = tsc.times
                newTs.values = tsc.values

                results = HecDss.open(resultsDssFile)
                results.put(newTs)
                results.done()
        else:
            print 'here'
    fid.done()

def archiveRandomHindcasts(tempDb, resultsDir, aepList, n, patternName, recordID, templateDb):

    nStr = str(n).zfill(3)

    archiveDbFile = r"%s\RR-%s_results.db" %(resultsDir, patternName)
    archiveDb = SqliteDatabase(archiveDbFile, SqliteDatabase.CREATION_MODE.CREATE_NEW_OR_OPEN_EXISTING_UPDATE)

    for aep in aepList:

        # Define version identifier used to query the template database
        version = 'C:000%s|%s' %(aep, patternName)
        versionID = VersionIdentifier("american.FOLSOM", "flow",version)

        # Query the template database for this AEP
        eTs = tempDb.getEnsembleTimeSeries(versionID)
        issueDates = list(eTs.getIssueDates())

        # Define output ensemble time series
        randomID = 'C:000%s|RR-%s-%s' %(aep, patternName, nStr)
        newEts = EnsembleTimeSeries(recordID, "cfs","PER-AVER", randomID)

        for issueDate in issueDates:

            # Get ensemble from the full ensemble time series
            e = eTs.getEnsemble(issueDate)
            randomMember = e.getValues()

            # Define new ensemble for the randomly selected member
            selectedEnsemble = Ensemble(issueDate, randomMember, e.getStartDateTime(), Duration.ofHours(1), "cfs" )

            # Add selected Ensemble to random ensemble database
            newEts.addEnsemble(selectedEnsemble)

        archiveDb.write(newEts)

    tempDb.deleteAllEnsemblesFromDB()
    tempDb.close()
    archiveDb.close()


memberLookup = dict(zip(range(0,41), range(1980,2021)))
patternLookup = {'1986':'X3WM', '1997':'Y2WM'}
simNameLookup = {'1986':'X3WM_HC', '1997':'Y2WM_HC'}
altNameLookup = {'1986':'RR-X3WM', '1997':'RR-Y2WM'}
patternSeeds = {'1986':12345, '1997':45612}

def main(baseRoot):

    watershedWkspFile = r"%s\models\R703F3_SOU_RR_20230717\R703F3_SOU_RR_20230717.wksp" %(baseRoot)
    resultsDir = r"%s\results" %(baseRoot)

    for pattern in ['1986', '1997']:

        # Define ResSim parameters
        altName = altNameLookup[pattern]
        simName = simNameLookup[pattern]
        patternName = patternLookup[pattern]

        simulationDssDir = os.path.join(baseRoot,'models' ,'R703F3_SOU_RR_20230717','rss',simName)
        cleanExtractDssPath = r"%s\staticFiles\%s\simulation.dss" %(baseRoot, patternName)

        # Set Up logging file to keep track of the selected members
        loggingFile = r"%s\logs\RR-%s_randomShuffle.log" %(baseRoot, patternName)
        loggerMain = myLogger("Begin Processing %s (%s)..." %(pattern, patternName), loggingFile)
        loggerMain.info("Starting Ensemble Member Shuffle...")

        # Initialize random number generator
        seed = patternSeeds[pattern]
        random = Random(seed)
        loggerMain.info("Initial Seed for random numbers: \t %s" %(seed))

        # Define Record ID for creating new ensemble time series
        recordID = RecordIdentifier("american.FOLSOM","flow")

        # Define AEP list for testing
        aepList = list(range(200,510,10))

        # Outer loop that defines how many times the ensemble member shuffle
        for n in range(0,4):

            loggerN = myLogger("Random shuffle number: %s" %(n), loggingFile)
            loggerN.info("Processing shuffle number %s of 100..." %(n))

            # Configure ResSim for headless work
            simMode, simRun = configureResSim(watershedWkspFile, simName, altName)

            # Move clean extract DSS file to rss folder
            shutil.copy(cleanExtractDssPath,simulationDssDir)

            # Define input database the full ensemble time series (41-members for each issue date)
            inputDB = r"%s\models\R703F3_SOU_RR_20230717\shared\new_db\%s_all_AEPs.db" %(baseRoot, pattern)
            db = SqliteDatabase(inputDB, SqliteDatabase.CREATION_MODE.OPEN_EXISTING_UPDATE)

            # Define Target database that will contain the random members (1-member for each issue date)
            templateDb = r"%s\models\R703F3_SOU_RR_20230717\shared\new_db\template.db" %(baseRoot)
            tempDb = SqliteDatabase(templateDb, SqliteDatabase.CREATION_MODE.CREATE_NEW_OR_OPEN_EXISTING_UPDATE)

            # Loop through each AEP
            for aep in aepList:

                # Log which is currently being worked on
                loggerAep = myLogger("scaling: %s" %(aep), loggingFile)
                loggerAep.info('Processing %s aep....'  %(aep))

                # Define version identifier used to query the full ensemble time series database
                version = "%s_%s" %(pattern, aep)
                versionID = VersionIdentifier("american.FOLSOM", "flow",version)

                # Query the full ensemble database for this AEP
                eTs = db.getEnsembleTimeSeries(versionID)

                # Define list of all forecast issuance
                issueDates = list(eTs.getIssueDates())
                membersChosen = []
                randomID = 'C:000%s|%s' %(aep, patternName)

                # Define ensemble time series for random ensemble members (1-member per issue date)
                newEts = EnsembleTimeSeries(recordID, "cfs","PER-AVER", randomID)

                for issueDate in issueDates:

                    # Randomly select a member [0-40]
                    randomMember = random.nextInt(41)
                    membersChosen.append(randomMember)

                    # Get ensemble from the full ensemble time series
                    e = eTs.getEnsemble(issueDate)
                    selectedMember = e.getValues()[randomMember:randomMember+1]

                    # Define new ensemble for the randomly selected member
                    selectedEnsemble = Ensemble(issueDate, selectedMember, e.getStartDateTime(), Duration.ofHours(1), "cfs" )

                    # Add selected Ensemble to random ensemble database
                    newEts.addEnsemble(selectedEnsemble)

                # Log which members were selected for each issue date
                shuffleLog = myLogger("shuffleLog: %s" %(aep), loggingFile)
                shuffleLog.info("IssueDates %s " %([issueDate.toString() for issueDate in issueDates]))
                shuffleLog.info('Members Selected: %s' %([memberLookup[memberChosen] for memberChosen in membersChosen]))

                # Write random ensemble time series to template db
                tempDb.write(newEts)

            tempDb.close()
            db.close()

            # Run ResSim
            simMode.computeRun(simRun, -1, Constants.TRUE, Constants.TRUE)
            ResSim.getCurrentModule().saveSimulation()
            ResSim.closeWatershed()
            HecDSSFileDataManager().closeAllFiles()

            # Archive process results
            archiveSimulationResults(aepList, pattern, patternName, resultsDir, simulationDssDir,n)

            tempDb = SqliteDatabase(templateDb, SqliteDatabase.CREATION_MODE.CREATE_NEW_OR_OPEN_EXISTING_UPDATE)
            archiveRandomHindcasts(tempDb, resultsDir, aepList, n, patternName, recordID, templateDb)

            # Delete simulation and template database for next simulation
            HecDSSFileDataManager().closeAllFiles()

            # Delete simulation files for next iteration
            os.remove(r"%s\simulation.dss" %(simulationDssDir))
            os.remove(templateDb)

    sys.exit("Finished Computing!")


if __name__ == '__main__':

    baseRoot = r"C:\workspace\Folsom\FolsomSOU-EnsembleMemberShuffle"

    main(baseRoot)