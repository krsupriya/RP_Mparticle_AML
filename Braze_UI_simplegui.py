import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import subprocess
import sys  # Import the sys module
import threading
import logging


# logging.basicConfig(filename=' data_sample_20240624T175602_429098.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
def on_close():
    print("Window closed")
    sys.exit()


def select_csv_file():
    # Ask for the CSV file
    csv_file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
    return csv_file_path


def select_xlsx_file():
    # Ask for the XLSX template file
    xlsx_file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
    return xlsx_file_path


# def compare_files():
#     csv_file_path = select_csv_file()
#     xlsx_file_path = select_xlsx_file()
#
#     if csv_file_path and xlsx_file_path:
#         # Load the CSV and XLSX files
#         df_csv = pd.read_csv(csv_file_path)
#         df_xlsx = pd.read_excel(xlsx_file_path)
#
#         # Compare columns
#         if set(df_csv.columns).issubset(df_xlsx.columns):
#             messagebox.showinfo("Success", "Successful trigger: Columns match!")
#             datafile = filedialog.askopenfilename(title="Select a file for upload",
#                                                   filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx")])
#             if datafile:
#                 try:
#                     # Absolute path to main.py
#                     main_py_path = "C:\\Users\\kumaris\\OneDrive - Hastings Insurance Services Ltd\\02. " \
#                                    "GitHub\\braze-bulk\\braze-bulk-upload\\main.py"
#                     # Run main.py with the selected file
#                     subprocess.run(["python", main_py_path, datafile], check=True)
#                 except subprocess.CalledProcessError as e:
#                     messagebox.showerror("Error", f"An error occurred while running main.py: {e}")
#                 except Exception as e:
#                     messagebox.showerror("Error", f"An unexpected error occurred: {e}")
#
#
# # Create the root window
# root = tk.Tk()
# root.title("File Comparer")
#
# # Create a frame
# frame = tk.Frame(root, bg="white")
# frame.pack(padx=20, pady=20)
#
# # Bind the button click event to the compare_files function
# compare_button = tk.Button(frame, text="Compare Files", command=lambda: threading.Thread(target=compare_files).start(),
#                            bg="blue", fg="white", font=("Arial", 12))
# compare_button.pack()
#
# # Set the protocol for the window close button to call the on_close function
# root.protocol("WM_DELETE_WINDOW", on_close)
#
# # Run the main loop
# root.mainloop()


import PySimpleGUI as sg
import threading


def compare_files():
    csv_file_path = select_csv_file()
    xlsx_file_path = select_xlsx_file()

    if csv_file_path and xlsx_file_path:
        # Load the CSV and XLSX files
        df_csv = pd.read_csv(csv_file_path)
        df_xlsx = pd.read_excel(xlsx_file_path)

        # Compare columns
        if set(df_csv.columns).issubset(df_xlsx.columns):
            messagebox.showinfo("Success", "Successful trigger: Columns match!")
            datafile = filedialog.askopenfilename(title="Select a file for upload",
                                                  filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx")])
            if datafile:
                try:
                    # Absolute path to main.py
                    main_py_path = "C:\\Users\\kumaris\\OneDrive - Hastings Insurance Services Ltd\\02. " \
                                   "GitHub\\braze-bulk\\braze-bulk-upload\\main.py"
                    # Run main.py with the selected file
                    subprocess.run(["python", main_py_path, datafile], check=True)
                except subprocess.CalledProcessError as e:
                    messagebox.showerror("Error", f"An error occurred while running main.py: {e}")
                except Exception as e:
                    messagebox.showerror("Error", f"An unexpected error occurred: {e}")

    pass


# def on_close():
#     # Your cleanup logic here
#     pass


layout = [
    [sg.Text('File Comparer', font=("Arial", 16))],
    [sg.Button('Compare Files', button_color=('white', 'blue'), font=("Arial", 12), key='-COMPARE-')]
]

window = sg.Window('File Comparer', layout, finalize=True)

while True:
    event, values = window.read()
    if event == sg.WIN_CLOSED:
        on_close()
        break
    elif event == '-COMPARE-':
        threading.Thread(target=compare_files).start()

window.close()
