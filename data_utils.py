import pandas as pd
from datetime import datetime
from os import listdir, getenv, path, makedirs
import logging
from os import path, makedirs
import os
import logging
from os import listdir, path
from datetime import datetime
import mparticle_utils as mp
from os import getenv
import sys
import os

# Add the directory containing mparticle_caller.py to the Python path
module_path = r'C:\Users\kumaris\OneDrive - Hastings Insurance Services Ltd\02. GitHub\braze-bulk\braze-bulk-upload'
if module_path not in sys.path:
    sys.path.append(module_path)
import mparticle_caller as caller #####start here


#og
# def load_data(filename):
#     chunksize = int(getenv('MPARTICLE_IMPORT_BATCH_SIZE'))
#     return pd.read_csv(f".data/{filename}", converters={i: str for i in range(0, 100)}, chunksize= chunksize)


# def load_data(filename):
#     chunksize = int(getenv('MPARTICLE_IMPORT_BATCH_SIZE'))
#     print(chunksize)
#     # Use the filename directly without prepending any path
#     return pd.read_csv(filename, converters={i: str for i in range(0, 100)}, chunksize=chunksize)



def load_data(filename):
    chunksize = getenv('MPARTICLE_IMPORT_BATCH_SIZE')
    print(f"Environment variable MPARTICLE_IMPORT_BATCH_SIZE: {chunksize}")
    chunksize = int(chunksize)
    print(chunksize)
    filename = r'C:\Users\kumaris\OneDrive - Hastings Insurance Services Ltd\02. GitHub\braze-bulk\braze-bulk-upload\data\data_sample.csv'

    # Print the full path to ensure it's correct
    print(f"1.Full path to the selected file: {os.path.abspath(filename)}")

    if not os.path.isfile(filename):
        raise FileNotFoundError(f"The file {filename} does not exist.")

    chunk_reader = pd.read_csv(filename, converters={i: str for i in range(0, 100)}, chunksize=chunksize)
    return chunk_reader


# print(f"Current working directory: {os.getcwd()}")
#
#     # Specify the correct path to the file
#
#     # Print the full path to ensure it's correct
# print(f"2. Full path to the selected file: {os.path.abspath(filename)}")



# def logfile(filename):
#     filename_noext = filename.split('.')[0]
#     now = datetime.now()
#     dt_string = now.strftime("%Y%m%dT%H:%M:%S_%f")
#     filename = f'{filename_noext}_{dt_string}'
#     if not path.exists("log"):
#       makedirs("log")
#     return open(f"log/{filename}.log", "w")

# Test for log file


# def testlogfile():
#     # Configure logging to write to a file with the INFO level
#     logging.basicConfig(filename='example.log', level=logging.INFO)
#
#     # Write a test log message
#     logging.info('This is a test message')
#
#     # Ensure the log directory exists
#     log_dir = "log"
#     if not path.exists(log_dir):
#         makedirs(log_dir)
#
#     # Define the log file path
#     log_file_path = path.join(log_dir, 'test.log')
#
#     # Write to the log file
#     try:
#         with open(log_file_path, 'a') as logfile:
#             logfile.write('Test log entry\n')
#             logfile.flush()  # Flush the write buffer to the file system
#             os.fsync(logfile.fileno())  # Ensure all internal file state is written to disk
#     except IOError as e:
#         print(f"An error occurred: {e}")
#
#     # Read from the log file
#     try:
#         with open(log_file_path, 'r') as logfile:
#             content = logfile.read()
#             print(content)
#     except IOError as e:
#         print(f"An error occurred while reading the file: {e}")
#
# # Call the function
# testlogfile()


# def logfile(filepath):
#     # Extract the base filename without the extension
#     filename_noext = path.basename(filepath).split('.')[0]
#     now = datetime.now()
#     dt_string = now.strftime("%Y%m%dT%H%M%S_%f")
#     # Construct the new filename using the base filename and the current timestamp
#     new_filename = f'{filename_noext}_{dt_string}.log'
#     log_dir = "log"
#     if not path.exists(log_dir):
#         makedirs(log_dir)
#     # Open the log file in the log directory
#     return open(path.join(log_dir, new_filename), "w")


def logfile(filepath):
    # Extract the base filename without the extension
    filename_next = path.basename(filepath).split('.')[0]
    now = datetime.now()
    dt_string = now.strftime("%Y%m%dT%H%M%S_%f")
    # Construct the new filename using the base filename and the current timestamp
    new_filename = f'{filename_next}_{dt_string}.log'
    print(new_filename)
    log_dir = "log"
    if not path.exists(log_dir):
        makedirs(log_dir)
    # Return the file path instead of an open file
    return path.join(log_dir, new_filename)


def process_chunks(chunks, logfile):
    mparticle_instance = mp.create_instance()
    resume_at = search_where_stopped(logfile)

    print("process start", resume_at)

    for chunk in chunks:
        process_chunk(chunk, mparticle_instance, logfile, resume_at)

    print("process end")


# For handling large datasets that need to be processed in chunks.
def process_chunk(chunk, mparticle_instance, logfile, resume_at=1):
    start = chunk.index.start + 1
    stop = chunk.index.stop
    if resume_at > stop:
        return
    cols = chunk.columns
    print(cols)
    logfile.write(f"[INFO] Ingesting chunk from line {start} to line {stop}\n")
    batches = []
    #setting single batch as true
    single = (getenv("MPARTICLE_IMPORT_SINGLE_BATCHES") == 'true')
    for i, row in enumerate(chunk.values):
        if resume_at > i + start:
            pass
        else:
            print("build start")
            data = build_data(row, cols, start + i, logfile)
            print("build end")

            if single:
                send_data(mparticle_instance, data, start + i, logfile)
            else:
                batches.append(data)
    if not single: send_data(mparticle_instance, batches, start, logfile)
    print("processchunk done")


def build_data(row, cols, reference_row, logfile):
    try:
        return mp.build_batch_object(row, cols)
    except:
        logfile.write(
            f"[Error] Data issue in CSV at line number #{reference_row}# failed to upload and caused import to stop.")
        raise


# This function attempts to send data to an mParticle instance and logs an error message if this process fails
# def send_data(mparticle_instance, data, reference_row, logfile):
#     try:
#         print("Entering try block")
#         print("trying call mparticle send_data start")
#         caller.call_mparticle(mparticle_instance, data)
#     except:
#         print("trying call mparticle send_data exception")
#
#         log_prefix = "Row data at " if (type(data) != list) else "Chunk starting at "
#         logfile.write(
#             f"[Error] {log_prefix}line number #{reference_row}# failed to upload and caused import to stop. Issue on "
#             f"import.")
#         raise
def send_data(mparticle_instance, data, reference_row, logfile):
    try:
        print("Entering try block")
        print("trying call mparticle send_data start")
        print(f"Data being passed: {data}")
        print(f"Reference row: {reference_row}")
        caller.call_mparticle(mparticle_instance, data)
        print("Successfully called mparticle send_data")
    except Exception as e:
        print("trying call mparticle send_data exception")
        print(f"Exception: {e}")

        log_prefix = "Row data at " if (type(data) != list) else "Chunk starting at "
        logfile.write(
            f"[Error] {log_prefix}line number #{reference_row}# failed to upload and caused import to stop. Issue on "
            f"import.")
        raise


# This function (search_where_stopped) is designed to handle resuming an import process where it left off in the event of an interruption.
# It does this by checking for older log files that start with the same name as the current log file, finding the last row that was imported from the most recent older log file,
# and then asking the user whether to resume the import at this line, start from the beginning of the file, or cancel the import.
# If there are no older log files, the function assumes that this is the first import and asks the user whether to start the import at line 1 or cancel the import.
# The function returns the line number where the import should resume.
# If the user chooses to cancel the import, the abort_import function is called with the current log file and the resume_at line number as arguments.
# def search_where_stopped(logfile):
#     current_logfile_name = logfile.name.split("/")[-1]
#     older_logfiles = listdir(f"./log")
#     older_logfiles = [x for x in older_logfiles if x.startswith(current_logfile_name[:-29])]
#     older_logfiles.remove(current_logfile_name)
#     resume_at = 1
#     try:
#         previous_logfile = max(older_logfiles)
#         resume_at = find_latest_row(previous_logfile)
#
#         x = "c"
#         if resume_at > 1:
#           x = input(f"[=====USER INPUT REQUIRED====]\n- Type \"yes\" if you want to resume at line {resume_at}\n- Type \"no\" if you want to start from the beginning of file\n- Type anything else to cancel\nAnswer: ")
#           match x:
#             case "yes":
#               logging.info("Resuming")
#             case "no":
#               resume_at = 1
#             case _:
#               abort_import(logfile,resume_at)
#         else:
#           x = input(f"[=====USER INPUT REQUIRED====]\n- Type \"yes\" to start importing at line {resume_at}\n- Type \"c\" anything else to cancel\nAnswer: ")
#           if x != "yes": abort_import(logfile,resume_at)
#
#     except ValueError:
#         logging.info("This is the very first import in records")
#         x = input(f"[=====USER INPUT REQUIRED====]\n- Type \"yes\" to start importing at line {resume_at}\n- Type \"c\" anything else to cancel\nAnswer: ")
#         if x != "yes": abort_import(logfile,resume_at)
#     return resume_at


# v2
# def search_where_stopped(logfile):
#     current_logfile_name = path.basename(logfile.name)
#     older_logfiles = listdir("./log")
#     older_logfiles = [x for x in older_logfiles if x.startswith(current_logfile_name[:-29])]
#
#     # Safely remove the current logfile from the list if it exists
#     if current_logfile_name in older_logfiles:
#         older_logfiles.remove(current_logfile_name)
#
#     resume_at = 1
#     try:
#         previous_logfile = max(older_logfiles, default=None)
#         if previous_logfile:
#             resume_at = find_latest_row(previous_logfile)
#             x = "c"
#             if resume_at > 1:
#                 x = input(
#                     f"[=====USER INPUT REQUIRED====]\n- Type \"yes\" if you want to resume at line {resume_at}\n- Type \"no\" if you want to start from the beginning of file\n- Type anything else to cancel\nAnswer: ")
#                 match x:
#                     case "yes":
#                         logging.info("Resuming")
#                     case "no":
#                         resume_at = 1
#                     case _:
#                         abort_import(logfile, resume_at)
#             else:
#                 x = input(
#                     f"[=====USER INPUT REQUIRED====]\n- Type \"yes\" to start importing at line {resume_at}\n- Type \"c\" anything else to cancel\nAnswer: ")
#                 if x != "yes": abort_import(logfile, resume_at)
#         else:
#             logging.info("No previous log files found.")
#
#
#     except ValueError:
#         logging.info("This is the very first import in records")
#         x = input(
#             f"[=====USER INPUT REQUIRED====]\n- Type \"yes\" to start importing at line {resume_at}\n- Type \"c\" anything else to cancel\nAnswer: ")
#         if x != "yes": abort_import(logfile, resume_at)
#     return resume_at


# Define a function to find the latest row in the logfile
# def find_latest_row(logfile):
#     with open(f'log/{logfile}', 'r') as f:
#         try:
#             last_line = f.readlines()[-1]
#             return int(last_line.split("#")[1])
#         except:
#             return 1


# -----02/08 uncomment
# def find_latest_row(logfile):
#     with open(f'log/{logfile}', 'r') as f:
#         try:
#             last_line = f.readlines()[-1]
#             result = int(last_line.split("#")[1])
#             print(f"Result: {result}")  # Add this line
#             return result
#         except Exception as e:  # Modify this line to catch the exception
#             print(f"Error: {e}")  # Add this line
#             return 1

# -----02/08 uncomment

# Define a function to abort the import
# def abort_import(logfile, line):
#     logging.warning("Script stopped by user")
#     logfile.write(f"[Warning] The user prevented the resume at line #{line}#. Nothing imported")
#     logfile.close()
#     exit()


# Define the function search_where_stopped
# def search_where_stopped(logfile_name):
#     current_logfile_name = path.basename(logfile_name)
#     older_logfiles = listdir("./log")
#     older_logfiles = [x for x in older_logfiles if x.startswith(current_logfile_name[:-29])]
#
#     # Safely remove the current logfile from the list if it exists
#     if current_logfile_name in older_logfiles:
#         older_logfiles.remove(current_logfile_name)
#
#     resume_at = 1
#     previous_logfile = max(older_logfiles, default=None)
#     if previous_logfile:
#         resume_at = find_latest_row(previous_logfile)
#         if resume_at > 1:
#             user_input = input(
#                 f"[=====USER INPUT REQUIRED====]\n- Type \"yes\" if you want to resume at line {resume_at}\n- Type \"no\" if you want to start from the beginning of file\n- Type anything else to cancel\nAnswer: ")
#             if user_input == "yes":
#                 logging.info("Resuming")
#             elif user_input == "no":
#                 resume_at = 1
#             else:
#                 abort_import(logfile_name, resume_at)
#         else:
#             user_input = input(
#                 f"[=====USER INPUT REQUIRED====]\n- Type \"yes\" to start importing at line {resume_at}\n- Type anything else to cancel\nAnswer: ")
#             if user_input != "yes":
#                 abort_import(logfile_name, resume_at)
#     else:
#         logging.info("No previous log files found.")
#     print("search_where_stopped done")

# 02/08
# def search_where_stopped(logfile_name):
#     current_logfile_name = path.basename(logfile_name)
#     older_logfiles = listdir("./log")
#     older_logfiles = [x for x in older_logfiles if x.startswith(current_logfile_name[:-29])]
#
#     # Safely remove the current logfile from the list if it exists
#     if current_logfile_name in older_logfiles:
#         older_logfiles.remove(current_logfile_name)
#
#     resume_at = 1
#     previous_logfile = max(older_logfiles, default=None)
#     if previous_logfile:
#         print(f"Calling find_latest_row with: {previous_logfile}")  # Add this line
#
#         resume_at = find_latest_row(previous_logfile)
#         if resume_at is not None and resume_at > 1:
#
#             user_input = input(
#                 f"[=====USER INPUT REQUIRED====]\n- Type \"yes\" if you want to resume at line {resume_at}\n- Type \"no\" if you want to start from the beginning of file\n- Type anything else to cancel\nAnswer: ")
#             if user_input == "yes":
#                 print("testting writing")
#                 logging.info("Resuming")
#             elif user_input == "no":
#                 resume_at = 1
#             else:
#                 abort_import(logfile_name, resume_at)
#         else:
#             user_input = input(
#                 f"[=====USER INPUT REQUIRED====]\n- Type \"yes\" to start importing at line {resume_at}\n- Type anything else to cancel\nAnswer: ")
#             if user_input != "yes":
#                 abort_import(logfile_name, resume_at)
#     else:
#         logging.info("No previous log files found.")
#     # print("search_where_stopped done")


def abort_import(logfile_name, line):
    logging.warning("Script stopped by user")
    with open(logfile_name, 'a') as logfile:
        logfile.write(f"[Warning] The user prevented the resume at line #{line}#. Nothing imported")
    exit()


# def find_latest_row(logfile_name):
#     with open(f'log/{logfile_name}', 'r') as f:
#         try:
#             last_line = f.readlines()[-1]
#             return int(last_line.split("#")[1])
#         except:
#             return 1

def find_latest_row(logfile_name):
    try:
        with open(f'log/{logfile_name}', 'r') as f:
            lines = f.readlines()
            if not lines:
                return 1  # Return 1 if the file is empty
            last_line = lines[-1]
            return int(last_line.split("#")[1])
    except (IndexError, ValueError) as e:
        logging.error(f"Error processing logfile {logfile_name}: {e}")
        return 1
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return 1


from pathlib import Path


def search_where_stopped(logfile):
    logfile_name = Path(logfile.name)

    current_logfile_name = path.basename(logfile_name)
    older_logfiles = listdir("./log")
    older_logfiles = [x for x in older_logfiles if x.startswith(current_logfile_name[:-29])]

    # Safely remove the current logfile from the list if it exists
    if current_logfile_name in older_logfiles:
        older_logfiles.remove(current_logfile_name)

    resume_at = 1
    previous_logfile = max(older_logfiles, default=None)
    if previous_logfile:
        print(f"Calling find_latest_row with: {previous_logfile}")  # Add this line

        resume_at = find_latest_row(previous_logfile)
        print(f"resume_at: {resume_at}")  # Debugging output

        if resume_at is not None and resume_at > 1:
            user_input = input(
                f"[=====USER INPUT REQUIRED====]\n- Type \"yes\" if you want to resume at line {resume_at}\n- Type \"no\" if you want to start from the beginning of file\n- Type anything else to cancel\nAnswer: ")
            if user_input == "yes":
                print("testing writing")
                print("Resuming")
            elif user_input == "no":
                resume_at: int = 1
            else:
                abort_import(logfile_name, resume_at)
        else:
            user_input = input(
                f"[=====USER INPUT REQUIRED====]\n- Type \"yes\" to start importing at line {resume_at}\n- Type anything else to cancel\nAnswer: ")
            if user_input != "yes":
                abort_import(logfile_name, resume_at)
    else:
        print("No previous log files found.")

    return resume_at
