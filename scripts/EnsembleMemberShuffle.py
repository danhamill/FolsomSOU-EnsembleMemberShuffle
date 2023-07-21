# from hec.script import ResSim
# from hec.script import Constants

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

# def configureResSim(watershedWkspFile, simName, altName):
#
#     #  Res Sim only likes unix-style path
#     watershedWkspFile = watershedWkspFile.replace(os.sep, "/")
#     ResSim.openWatershed(watershedWkspFile)
#     ResSim.selectModule('Simulation')
#     simMode = ResSim.getCurrentModule()
#
#     # Not sure what this does, but this is the only way to open a simulation
#     simMode.resetWorkspace()
#     simMode.openSimulation(simName)
#     simulation = simMode.getSimulation()
#
#     # force compute everything
#     simulation.setComputeAll(1)
#     simRun = simulation.getSimulationRun(altName)
#     return simMode, simRun

def myLogger(name, path):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(path, 'a')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


memberLookup = dict(zip(range(0,41), range(1980,2021)))
patternLookup = {'1986':'X3WM', '1997':'Y3WM'}
simNameLookup = {'1986':'X3WM_HC', '1997':'Y3WM_HC'}
altNameLookup = {'1986':'RR-X3WM', '1997':'RR-Y3WM'}

def main(pattern, watershedWkspFile):

    baseRoot = r"C:\workspace\Folsom\FolsomSOU-EnsembleMemberShuffle"

    # Define ResSim parameters
    altName = altNameLookup[pattern]
    simName = simNameLookup[pattern]
    patternName = patternLookup[pattern]

    simulationDssDir = os.path.join(baseRoot,'models' ,'R703F3_SOU_RR_20230717','rss',simName)
    cleanExtractDssPath = r"%s\staticFiles\%s\simulation.dss" %(baseRoot, patternName)

    # Configure ResSim for headless work
    # simMode, simRun = configureResSim(watershedWkspFile, simName, altName)

    # Set Up logging file to keep track of the selected members
    loggingFile = r"logs\randomShuffle.log"
    loggerMain = myLogger("main", loggingFile)
    loggerMain.info("Starting Ensemble Member Shuffle...")

    seed = 12345
    random = Random(seed)
    loggerMain.info("Inital Seed for random numbers: \t %s" %(seed))

    # Define Record ID for creating new ensemble time series
    recordID = RecordIdentifier("american.FOLSOM","flow")

    # Outer loop that defines how many times the ensemble member shuffle
    for n in range(0,100):

        # Move clean extract DSS file to rss folder
        shutil.copy(cleanExtractDssPath,simulationDssDir)

        # Define input database the full ensemble time series (41-members for each issue date)
        inputDB = r"%s\models\R703F3_SOU_RR_20230717\shared\new_db\%s_all_AEPs.db" %(baseRoot, pattern)
        db = SqliteDatabase(inputDB, SqliteDatabase.CREATION_MODE.OPEN_EXISTING_UPDATE)

        # Define Target database that will contain the random members (1-member for each issue date)
        templateDb = r"%s\models\R703F3_SOU_RR_20230717\shared\new_db\template.db" %(baseRoot)
        tempDb = SqliteDatabase(templateDb, SqliteDatabase.CREATION_MODE.CREATE_NEW_OR_OPEN_EXISTING_UPDATE)

        # Loop through each AEP
        for aep in range(200,510,10):

            # Log which is currently being worked on
            loggerAep = myLogger("scaling: %s" %(aep), loggingFile)
            loggerAep.info('Processing %s aep....'  %(aep))

            # Define version identifier used to query the full ensemble time series database
            version = "1986_%s" %(aep)
            versionID = VersionIdentifier("american.FOLSOM", "flow",version)

            # Query the full ensemble database for this AEP
            eTs = db.getEnsembleTimeSeries(versionID)

            # Define list of all forecast issueances
            issueDates = list(eTs.getIssueDates())
            membersChosen = []
            randomID = 'C:000%s|%s' %(aep, patternName)

            # Define ensemble time series for random ensemble members (1-member per issue date)
            newEts = EnsembleTimeSeries(recordID, "kcfs","PER-AVER", randomID)

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


            tempDb.write(newEts)

        tempDb.close()
        db.close()

        # Run ResSim
        # simMode.computeRun(simRun, -1, Constants.TRUE, Constants.TRUE)
        # ResSim.getCurrentModule().saveSimulation()

        # Post process results

        # Delete simulaiton and template database for next simulation
        HecDSSFileDataManager().closeAllFiles()
        os.remove(simulationDssFile)

if __name__ == '__main__':

    pattern = '1986'
    watershedWkspFile = r"C:\workspace\Folsom\FolsomSOU-EnsembleMemberShuffle\models\R703F3_SOU_RR_20230717\R703F3_SOU_RR_20230717.wksp"
    main(pattern, watershedWkspFile)