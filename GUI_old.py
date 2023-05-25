import tkinter as tk
from tkinter import filedialog, messagebox
from subprocess import Popen, PIPE, STDOUT
import json
import os
import threading
import glob

class Application(tk.Frame):
    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        self.grid(sticky="nsew")
        self.process = None  # Add this line to store the Popen process
        self.create_widgets()
        self.master.configure(bg="grey20")  # Set the background color of the main window

    def find_python_interpreter(self):
        script_dir = os.path.dirname(os.path.realpath(__file__))
        venv_path = os.path.join(script_dir, 'venv')

        if os.name == 'nt':  # Windows
            python_path = os.path.join(venv_path, 'Scripts', 'python.exe')
        else:  # POSIX
            python_path = os.path.join(venv_path, 'bin', 'python')

        if os.path.exists(python_path):
            return python_path
        else:
            return None

    def create_widgets(self):
        self.master.grid_columnconfigure(0, weight=1)
        self.master.grid_rowconfigure(0, weight=1)

        # PanedWindow as a draggable divider
        self.paned_window = tk.PanedWindow(self.master, orient="horizontal")
        self.paned_window.grid(row=0, column=0, sticky="nsew")  # Changed pack to grid

        # File selection frame
        self.file_select_frame = tk.Frame(self.paned_window)
        self.paned_window.add(self.file_select_frame, width=300)

        self.select_button = tk.Button(self.file_select_frame, text="Select JSON Config", command=self.load_file)
        self.select_button.pack(side="top", fill="x")

        self.file_list = tk.Listbox(self.file_select_frame)
        self.file_list.pack(side="top", fill="both", expand=True)
        self.refresh_file_list()

        # Output frame
        self.output_frame = tk.Frame(self.paned_window)
        self.paned_window.add(self.output_frame)

        self.run_button = tk.Button(self.output_frame, text="Run Configured Jobs", command=self.run_command)
        self.run_button.grid(row=0, column=0, sticky="ew")  # Changed pack to grid

        self.scrollbar = tk.Scrollbar(self.output_frame)
        self.scrollbar.grid(row=1, column=2, sticky="ns")

        self.output_text = tk.Text(self.output_frame, wrap="word", yscrollcommand=self.scrollbar.set, bg="black",
                                   fg="white")
        self.output_text.grid(row=1, column=0, columnspan=2, sticky="nsew")
        self.scrollbar.config(command=self.output_text.yview)

        self.output_frame.grid_columnconfigure(0, weight=1)
        self.output_frame.grid_rowconfigure(1, weight=1)

        self.stop_button = tk.Button(self.output_frame, text="Stop", command=self.stop_command, bg="red", fg="white",
                                     activebackground="red3", activeforeground="white")
        self.stop_button.grid(row=0, column=1, sticky="e")  # Added this line

        # Modify your widgets to have dark backgrounds and light blue highlights
        self.file_select_frame.configure(bg="grey20")
        self.select_button.configure(bg="grey20", fg="light blue", activebackground="grey30",
                                     activeforeground="light blue")
        self.file_list.configure(bg="grey20", fg="light blue", selectbackground="grey30", selectforeground="light blue")
        self.output_frame.configure(bg="grey20")
        self.run_button.configure(bg="grey20", fg="light blue", activebackground="grey30",
                                  activeforeground="light blue")
        self.output_text.configure(bg="grey20", fg="light blue", insertbackground="light blue")

        # Interpreter widget
        self.interpreter_frame = tk.Frame(self.master, bg="grey20")
        self.interpreter_frame.grid(row=2, column=0, sticky="ew")

        self.interpreter_label = tk.Label(self.interpreter_frame, text="Interpreter:", bg="grey20", fg="light blue")
        self.interpreter_label.pack(side="left")

        self.interpreter_entry = tk.Entry(self.interpreter_frame, bg="grey20", fg="light blue",
                                          insertbackground="light blue")
        self.interpreter_entry.pack(side="left", fill="x", expand=True)

        self.interpreter_button = tk.Button(self.interpreter_frame, text="...", command=self.select_interpreter,
                                            bg="grey20", fg="light blue", activebackground="grey30",
                                            activeforeground="light blue")
        self.interpreter_button.pack(side="right")

        # Populate the interpreter_entry with the path of the found interpreter or 'python'
        self.interpreter_entry.insert(0, self.find_python_interpreter() or 'python')

    def select_interpreter(self):
        interpreter = filedialog.askopenfilename()
        if interpreter:
            self.interpreter_entry.delete(0, tk.END)
            self.interpreter_entry.insert(0, interpreter)

    def find_python_interpreter(self):
        script_dir = os.path.dirname(os.path.realpath(__file__))
        venv_path = os.path.join(script_dir, 'venv')

        if os.name == 'nt':  # Windows
            python_path = os.path.join(venv_path, 'Scripts', 'python.exe')
        else:  # POSIX
            python_path = os.path.join(venv_path, 'bin', 'python')

        if os.path.exists(python_path):
            return python_path
        else:
            return None

    def load_file(self):
        filename = filedialog.askopenfilename(filetypes=(("JSON files", "*.json"), ("all files", "*.*")))
        if not filename.endswith('.json'):
            messagebox.showerror("Invalid file", "Please select a JSON file.")
        else:
            self.file_list.insert(tk.END, filename)

    def refresh_file_list(self):
        json_files = glob.glob('config*.json')  # Modified the glob pattern
        for file in json_files:
            self.file_list.insert(tk.END, file)

    def run_command(self):
        selected_file = self.file_list.get(self.file_list.curselection())
        if not selected_file:
            messagebox.showerror("No file selected", "Please select a JSON file before running.")
        else:
            interpreter = self.interpreter_entry.get()  # Get the selected interpreter
            command = f"{interpreter} main.py --config {selected_file}"  # Use the selected interpreter in the command
            self.execute(command)
            self.process = Popen(command, stdout=PIPE, stderr=STDOUT, shell=True)  # Modify this line
            threading.Thread(target=self.stream_output, args=(self.process,)).start()

    def stop_command(self):
        if self.process:
            self.process.terminate()
            self.process = None

    def execute(self, command):
        process = Popen(command, stdout=PIPE, stderr=STDOUT, shell=True)
        threading.Thread(target=self.stream_output, args=(process,)).start()

    def stream_output(self, process):
        for c in iter(lambda: process.stdout.read(1), ''):
            self.output_text.insert('end', c)
            self.output_text.see('end')
            self.master.update_idletasks()

root = tk.Tk()
root.title("JsonMapper")
app = Application(master=root)
app.mainloop()