# import logging
# import data_utils as du
# from file_utils import prompt_data_files
# import sys


# def main():
#
#   datafile = prompt_data_files()
# def main(datafile=None):
#   if datafile is None and len(sys.argv) > 1:
#     print("Choose the file for data upload")
#     datafile = sys.argv[1]
#   elif datafile is None:
#     datafile = prompt_data_files()
#   print("Test1")
#   chunk_reader = du.load_data(datafile)
#   print("Test2")
#
#   logfile = du.logfile(datafile)
#   print("Test3")
#
#   try:
#     print("testload")
#     du.process_chunks(chunk_reader, logfile)
#   except Exception as e:
#     logging.error(f"An error occurred during the import, check the latest log for more information\nDetails:\n{str(e)}")
#   finally:
#     logfile.close()


import logging
import sys
from fileinput import filename
import data_utils as du
from file_utils import prompt_data_files
import threading


def main(datafile=None):
    if datafile is None and len(sys.argv) > 1:
        print("Choose the file for data upload")
        datafile = sys.argv[1]
    elif datafile is None:
        datafile = prompt_data_files()
    print("Test1")
    chunk_reader = du.load_data(datafile)
    print("Test2")

    logfile_name = du.logfile(datafile)
    print(f"Log file: {logfile_name}")

    logfile = None  # Initialize logfile as None

    try:
        logfile = open(logfile_name, 'w')

        print("Test3")
        du.process_chunks(chunk_reader, logfile)
        print("Test3 end")

    except Exception as e:
        logging.error(
            f"An error occurred during the import, check the latest log for more information\nDetails:\n{str(e)}")
    finally:
        if logfile:
            # Check if logfile is not None before trying to close it
            logfile.close()


if __name__ == "__main__":
    # import requests
    #
    # response = requests.get('https://s2s.eu1.mparticle.com/v2', verify=False)

    main()


