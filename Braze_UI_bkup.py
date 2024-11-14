# import os
# from dotenv import load_dotenv
# import tkinter as tk
# from tkinter import filedialog, messagebox
# import pandas as pd
# # import data_utils as du
# import logging
# # Load environment variables
# load_dotenv()
#
# # Access the environment variables
# MPARTICLE_IMPORT_BATCH_SIZE = int(os.getenv('MPARTICLE_IMPORT_BATCH_SIZE', 100))
# MPARTICLE_IMPORT_SINGLE_BATCHES = os.getenv('MPARTICLE_IMPORT_SINGLE_BATCHES') == 'true'
# SLEEP_BETWEEN_REQUESTS_SECONDS = float(os.getenv('SLEEP_BETWEEN_REQUESTS_SECONDS', 0.02))
# MPARTICLE_ENVIRONMENT = os.getenv('MPARTICLE_ENVIRONMENT', 'development')
# MPARTICLE_DATA_PLAN_NAME = os.getenv('MPARTICLE_DATA_PLAN_NAME', 'main')
# MPARTICLE_DATA_PLAN_VERSION = int(os.getenv('MPARTICLE_DATA_PLAN_VERSION', 3))
# MPARTICLE_API_KEY = os.getenv('MPARTICLE_API_KEY')
# MPARTICLE_API_SECRET = os.getenv('MPARTICLE_API_SECRET')
# MPARTICLE_DEBUG = os.getenv('MPARTICLE_DEBUG') == 'true'
# MPARTICLE_HOST = os.getenv('MPARTICLE_HOST', 'https://s2s.eu1.mparticle.com/v2')
#
#
#
#
#
#
# class CustomDialog(tk.Toplevel):
#     def __init__(self, parent):
#         super().__init__(parent)
#         self.title("Error")
#         self.geometry("300x200")
#
#         # Create a label
#         self.label = tk.Label(self, text="Some column names are not in the CSV file.")
#         self.label.pack(pady=10)
#
#         # Create a button
#         self.button = tk.Button(self, text="OK", command=self.destroy)
#         self.button.pack(pady=10)
#
#         self.transient(parent)
#         self.grab_set()
#
# def on_button_click(root):
#     file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
#     if file_path:
#         df = pd.read_csv(file_path)
#         column_names = df.columns.tolist()
#         # df_temp is a DataFrame loaded previously with the template columns
#         if set(column_names).issubset(df.columns):
#             # Process the data
#             pass
#         else:
#             dialog = CustomDialog(root)
#             root.wait_window(dialog)
#
# # Create the root window
# root = tk.Tk()
# # root.withdraw()  # Hide the root window
#
# # Bind the button click event to the on_button_click function
# button = tk.Button(root, text="Open CSV", command=lambda: on_button_click(root))
# button.pack()
#
# # Run the main loop
# root.mainloop()




# import os
# from dotenv import load_dotenv
# import tkinter as tk
# from tkinter import filedialog, messagebox
# import pandas as pd
#
# # Load environment variables
# load_dotenv()
#
# # Access the environment variables
# MPARTICLE_IMPORT_BATCH_SIZE = int(os.getenv('MPARTICLE_IMPORT_BATCH_SIZE', 100))
# MPARTICLE_IMPORT_SINGLE_BATCHES = os.getenv('MPARTICLE_IMPORT_SINGLE_BATCHES') == 'true'
# SLEEP_BETWEEN_REQUESTS_SECONDS = float(os.getenv('SLEEP_BETWEEN_REQUESTS_SECONDS', 0.02))
# MPARTICLE_ENVIRONMENT = os.getenv('MPARTICLE_ENVIRONMENT', 'development')
# MPARTICLE_DATA_PLAN_NAME = os.getenv('MPARTICLE_DATA_PLAN_NAME', 'main')
# MPARTICLE_DATA_PLAN_VERSION = int(os.getenv('MPARTICLE_DATA_PLAN_VERSION', 3))
# MPARTICLE_API_KEY = os.getenv('MPARTICLE_API_KEY')
# MPARTICLE_API_SECRET = os.getenv('MPARTICLE_API_SECRET')
# MPARTICLE_DEBUG = os.getenv('MPARTICLE_DEBUG') == 'true'
# MPARTICLE_HOST = os.getenv('MPARTICLE_HOST', 'https://s2s.eu1.mparticle.com/v2')

# class CustomDialog(tk.Toplevel):
#     def __init__(self, parent):
#         super().__init__(parent)
#         self.title("Error")
#         self.geometry("300x200")
#
#         # Create a label
#         self.label = tk.Label(self, text="Some column names are not in the CSV file.")
#         self.label.pack(pady=10)
#
#         # Create a button
#         self.button = tk.Button(self, text="OK", command=self.destroy)
#         self.button.pack(pady=10)
#
#         self.transient(parent)
#         self.grab_set()

# def on_button_click(root):
#     # Ask for the CSV file
#     csv_file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
#     # Ask for the XLSX template file
#     xlsx_file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
#
#     if csv_file_path and xlsx_file_path:
#         # Load the CSV and XLSX files
#         df_csv = pd.read_csv(csv_file_path)
#         df_xlsx = pd.read_excel(xlsx_file_path)
#
#         # Compare columns
#         if set(df_csv.columns).issubset(df_xlsx.columns):
#             messagebox.showinfo("Success", "Successful trigger: Columns match!")
#         else:
#             messagebox.showwarning("Mismatch", "The columns do not match the template.")
#
# # Create the root window
# root = tk.Tk()
#
# # Bind the button click event to the on_button_click function
# button = tk.Button(root, text="Open Files", command=lambda: on_button_click(root))
# button.pack()
#
# # Run the main loop
# root.mainloop()



import os
from dotenv import load_dotenv
import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import subprocess
import tkinter as tk
from tkinter import ttk
# Load environment variables
load_dotenv()

# Access the environment variables
# Access the environment variables
MPARTICLE_IMPORT_BATCH_SIZE = int(os.getenv('MPARTICLE_IMPORT_BATCH_SIZE', 100))
MPARTICLE_IMPORT_SINGLE_BATCHES = os.getenv('MPARTICLE_IMPORT_SINGLE_BATCHES') == 'true'
SLEEP_BETWEEN_REQUESTS_SECONDS = float(os.getenv('SLEEP_BETWEEN_REQUESTS_SECONDS', 0.02))
MPARTICLE_ENVIRONMENT = os.getenv('MPARTICLE_ENVIRONMENT', 'development')
MPARTICLE_DATA_PLAN_NAME = os.getenv('MPARTICLE_DATA_PLAN_NAME', 'main')
MPARTICLE_DATA_PLAN_VERSION = int(os.getenv('MPARTICLE_DATA_PLAN_VERSION', 3))
MPARTICLE_API_KEY = os.getenv('MPARTICLE_API_KEY')
MPARTICLE_API_SECRET = os.getenv('MPARTICLE_API_SECRET')
MPARTICLE_DEBUG = os.getenv('MPARTICLE_DEBUG') == 'true'
MPARTICLE_HOST = os.getenv('MPARTICLE_HOST', 'https://s2s.eu1.mparticle.com/v2')

# class CustomDialog(tk.Toplevel):
#     def __init__(self, parent):
#         super().__init__(parent)
#         self.title("Error")
#         self.geometry("300x200")
#
#         # Create a label
#         self.label = tk.Label(self, text="Some column names are not in the CSV file.")
#         self.label.pack(pady=10)
#
#         # Create a button
#         self.button = tk.Button(self, text="OK", command=self.destroy)
#         self.button.pack(pady=10)
#
#         self.transient(parent)
#         self.grab_set()
# import tkinter as tk
# from tkinter import tix
#
# from PyQt5.QtWidgets import QApplication, QDialog, QVBoxLayout, QLabel, QPushButton
#
# class CustomDialog(QDialog):
#     def __init__(self, parent=None):
#         super().__init__(parent)
#
#         # Set window title
#         self.setWindowTitle("Error")
#
#         # Create a layout
#         layout = QVBoxLayout()
#         self.setLayout(layout)
#
#         # Create a label
#         self.label = QLabel("Some column names are not in the CSV file.")
#         layout.addWidget(self.label)
#
#         # Create a button
#         self.button = QPushButton("OK")
#         self.button.clicked.connect(self.accept)
#         layout.addWidget(self.button)
#
# if __name__ == "__main__":
#     app = QApplication([])
#     dialog = CustomDialog()
#     dialog.show()
#     app.exec_()
#
#
#
# def select_csv_file(root):
#     # Ask for the CSV file
#     csv_file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
#     return csv_file_path
#
# def select_xlsx_file(root):
#     # Ask for the XLSX template file
#     xlsx_file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
#     return xlsx_file_path
#
# def compare_files(root):
#     csv_file_path = select_csv_file(root)
#     xlsx_file_path = select_xlsx_file(root)
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
#                     main_py_path = "C:\\Users\\kumaris\\OneDrive - Hastings Insurance Services Ltd\\02. GitHub\\braze-bulk\\braze-bulk-upload\\main.py"
#                     # Run main.py with the selected file
#                     subprocess.run(["python", main_py_path, datafile], check=True)
#                 except subprocess.CalledProcessError as e:
#                     messagebox.showerror("Error", f"An error occurred while running main.py: {e}")
#                 except Exception as e:
#                     messagebox.showerror("Error", f"An unexpected error occurred: {e}")
#
# # Create the root window
# root = tk.Tk()
#
# # Bind the button click event to the compare_files function
# compare_button = tk.Button(root, text="Compare Files", command=lambda: compare_files(root))
# compare_button.pack()
#
# # Run the main loop
# root.mainloop()


# print("Current Working Directory:", os.getcwd())
# C:\Users\kumaris\OneDrive - Hastings Insurance Services Ltd\02. GitHub\braze-bulk\braze-bulk-upload\main.py
# C:\Users\kumaris\OneDrive - Hastings Insurance Services Ltd\02. GitHub\braze-bulk\braze-bulk-upload\Braze_UI.py


# style = ttk.Style()
# print(style.theme_names())  # prints all available themes
# style.theme_use('default')  # replace 'default' with a theme from the printed list

#______________________________________________________VERSION________________________________________________________________________

import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import subprocess
import sys

def select_csv_file():
    # Ask for the CSV file
    csv_file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
    return csv_file_path

def select_xlsx_file():
    # Ask for the XLSX template file
    xlsx_file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
    return xlsx_file_path

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
                    main_py_path = "C:\\Users\\kumaris\\OneDrive - Hastings Insurance Services Ltd\\02. GitHub\\braze-bulk\\braze-bulk-upload\\main.py"
                    # Run main.py with the selected file
                    subprocess.run(["python", main_py_path, datafile], check=True)
                except subprocess.CalledProcessError as e:
                    messagebox.showerror("Error", f"An error occurred while running main.py: {e}")
                except Exception as e:
                    messagebox.showerror("Error", f"An unexpected error occurred: {e}")

# Create the root window
root = tk.Tk()
root.title("File Comparer")

# Create a frame
frame = tk.Frame(root, bg="white")
frame.pack(padx=20, pady=20)

# Bind the button click event to the compare_files function
compare_button = tk.Button(frame, text="Compare Files", command=compare_files, bg="blue", fg="white", font=("Arial", 12))
compare_button.pack()

# Run the main loop
root.mainloop()
def on_close():
    print("Window closed")
    sys.exit()
root = tk.Tk()
root.protocol("WM_DELETE_WINDOW", on_close)
root.mainloop()