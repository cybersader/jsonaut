import threading

from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton, QVBoxLayout, QWidget, QFileDialog, QListWidget, \
    QSplitter, QTextEdit, QLabel, QLineEdit, QHBoxLayout
from PyQt5.QtCore import Qt, QObject, pyqtSignal
from PyQt5 import QtGui
from PyQt5.QtGui import QTextCursor
import sys
import os
import subprocess
import glob
import io
import sys
import re
from jaccard_index.jaccard import jaccard_index
from collections import deque


class EmittingStream(QObject):
    textWritten = pyqtSignal(str)

    def __init__(self, parent=None):
        super(EmittingStream, self).__init__(parent)
        self.io = io.StringIO()

    def write(self, text):
        self.io.write(text)
        self.textWritten.emit(text)


class TerminalEmulator(QTextEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setReadOnly(True)
        self.cursor = self.textCursor()
        self.cursor.movePosition(QTextCursor.Start)
        self.cursor.setPosition(0)
        self.cursor.setKeepPositionOnInsert(True)  # Prevents automatic scrolling
        self.last_position = 0
        self.is_overwriting = False

    def write(self, text):
        if text.endswith('\r'):
            self.is_overwriting = True
            self.cursor.setPosition(self.last_position)
            self.setTextCursor(self.cursor)
        else:
            if self.is_overwriting:
                for _ in range(self.cursor.position(), self.last_position):
                    self.cursor.deleteChar()
                self.is_overwriting = False
            self.cursor.insertText(text)
            self.last_position = self.cursor.position()
            self.setTextCursor(self.cursor)

    def flush(self):
        pass


class Worker(QObject):
    output_line = pyqtSignal(str)
    finished = pyqtSignal()  # new signal

    def __init__(self, process):
        super().__init__()
        self.process = process

    def read_output(self):
        while True:
            line = self.process.stdout.readline()
            if not line and self.process.poll() is not None:
                break
            self.output_line.emit(line.strip())
        self.finished.emit()  # emit the finished signal when the process finishes


class Application(QMainWindow):
    def __init__(self, keywords=None, past_lines=3):
        super().__init__()

        self.setStyleSheet("""
                    QMainWindow {
                        background-color: #2b2b2b;
                    }

                    QPushButton {
                        color: #b1b1b1;
                        background-color: #31363b;
                    }

                    QPushButton#stop {
                        color: #FFFFFF;
                        background-color: #8b0000;
                    }

                    QPushButton#run {
                        color: #FFFFFF;
                        background-color: #2673d1;
                    }

                    QListWidget, QTextEdit, QLineEdit {
                        background-color: #232629;
                        color: #eff0f1;
                    }

                    QLabel {
                        color: #eff0f1;
                    }
                """)
        self.setWindowTitle("JsonMapper")

        self.output_text = TerminalEmulator()
        self.emitting_stream = EmittingStream(self)
        self.emitting_stream.textWritten.connect(self.output_text.write)
        sys.stdout = self.emitting_stream

        self.worker = None
        self.past_lines = past_lines
        self.keywords = keywords if keywords else []
        self.previous_lines = []  # List to store past lines

        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.layout = QVBoxLayout(self.central_widget)

        self.splitter = QSplitter(Qt.Horizontal)

        self.file_list = QListWidget()
        self.refresh_file_list()
        self.splitter.addWidget(self.file_list)
        self.file_list.itemClicked.connect(self.file_list_clicked)  # connect the itemClicked signal to a new function

        self.output_text = QTextEdit()
        self.output_text.setReadOnly(True)
        self.splitter.addWidget(self.output_text)

        self.layout.addWidget(self.splitter)

        self.run_button = QPushButton("Run Configured Jobs")
        self.run_button.setObjectName("run")  # add this line to apply specific style
        self.run_button.clicked.connect(self.run_command)
        self.layout.addWidget(self.run_button)

        self.interpreter_label = QLabel("Interpreter (python.exe):", self)
        self.interpreter_line_edit = QLineEdit(self)
        self.interpreter_button = QPushButton("Select", self)
        self.interpreter_button.clicked.connect(self.interpreter_dialog)

        interpreter_layout = QHBoxLayout()
        interpreter_layout.addWidget(self.interpreter_label)
        interpreter_layout.addWidget(self.interpreter_line_edit)
        interpreter_layout.addWidget(self.interpreter_button)
        self.layout.addLayout(interpreter_layout)

        self.stop_button = QPushButton("Stop")
        self.stop_button.setObjectName("stop")  # add this line to apply specific style
        self.stop_button.clicked.connect(self.stop_command)
        self.layout.addWidget(self.stop_button)

        self.find_python_interpreter()

        self.file_label = QLabel("JSON Config File:", self)
        self.file_line_edit = QLineEdit(self)  # new line to add QLineEdit
        self.file_button = QPushButton("Select JSON Config", self)
        self.file_button.clicked.connect(self.file_dialog)

        file_layout = QHBoxLayout()
        file_layout.addWidget(self.file_label)
        file_layout.addWidget(self.file_line_edit)  # add QLineEdit to the layout
        file_layout.addWidget(self.file_button)
        self.layout.insertLayout(1, file_layout)  # changed from addLayout to insertLayout to place it at the top

        self.process = None

    def file_dialog(self):
        filename = QFileDialog.getOpenFileName(self, 'Select File')
        if filename[0]:
            if filename[0].endswith(".json"):
                self.file_line_edit.setText(filename[0])  # set QLineEdit text to selected file path
            elif filename[0].endswith(".py"):
                self.interpreter_line_edit.setText(filename[0])

    # new function to handle itemClicked event
    def file_list_clicked(self, item):
        self.file_line_edit.setText(item.text())

    def remove_trailing_whitespace(self):
        # Save the current scrollbar position
        scrollbar_position = self.output_text.verticalScrollBar().value()

        text = self.output_text.toPlainText()
        self.output_text.setPlainText(text.rstrip())

        # Restore the scrollbar position
        self.output_text.verticalScrollBar().setValue(scrollbar_position)

    def append_finished_text(self):
        # Save the current scrollbar position
        scrollbar_position = self.output_text.verticalScrollBar().value()

        self.output_text.setTextColor(QtGui.QColor("green"))
        self.output_text.append("\n[ JOBS FINISHED ]\n")
        self.output_text.setTextColor(QtGui.QColor("white"))  # assuming the original color was white

        # Restore the scrollbar position
        self.output_text.verticalScrollBar().setValue(scrollbar_position)

    def append_failed_text(self):
        # Save the current scrollbar position
        scrollbar_position = self.output_text.verticalScrollBar().value()

        self.output_text.setTextColor(QtGui.QColor("red"))
        self.output_text.append("\n[ JOBS FAILED ]\n")
        self.output_text.setTextColor(QtGui.QColor("white"))  # assuming the original color was white

        # Restore the scrollbar position
        self.output_text.verticalScrollBar().setValue(scrollbar_position)

    def interpreter_dialog(self):
        filename = QFileDialog.getOpenFileName(self, 'Select Interpreter')
        if filename[0]:
            self.interpreter_line_edit.setText(filename[0])

    def refresh_file_list(self):
        for file in glob.glob('config*.json'):
            self.file_list.addItem(file)

    def find_python_interpreter(self):
        for root, dirs, files in os.walk(os.getcwd()):
            for file in files:
                if file == "python.exe" or file == "python3.exe":
                    return self.interpreter_line_edit.setText(os.path.join(root, file))
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, "python.exe")
            if os.path.isfile(exe_file) and os.access(exe_file, os.X_OK):
                self.interpreter_line_edit.setText(exe_file)
                return
        return sys.executable

    def run_command(self):
        selected_file = self.file_line_edit.text()
        self.job_stopped = False  # reset the flag
        interpreter = self.interpreter_line_edit.text()
        command = [interpreter, "main.py", "--config", selected_file]
        self.process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                        encoding='utf-8')

        # Add validation for interpreter and selected file
        if not os.path.isfile(interpreter) or not interpreter.lower().endswith(".exe"):
            print("Invalid interpreter!")
            return
        if not os.path.isfile(selected_file) or not selected_file.lower().endswith(".json"):
            print("Invalid file!")
            return

        self.output_text.clear()

        # create the Worker object and connect the signal to the slot
        self.worker = Worker(self.process)
        self.worker.output_line.connect(self.append_output_line)

        # connect the finished signal to the remove_trailing_whitespace method
        self.worker.finished.connect(self.remove_trailing_whitespace)

        # connect the finished signal to the append_finished_text method
        self.worker.finished.connect(self.append_finished_text)

        # start the new thread with Worker.read_output as the target function
        threading.Thread(target=self.worker.read_output).start()

    def check_process_exit(self):
        self.process.wait()
        return_code = self.process.returncode

        self.remove_trailing_whitespace()

        if not self.job_stopped:
            if return_code == 0:
                self.append_finished_text()
            else:
                self.append_failed_text()

    def append_output_line(self, line):
        try:
            scrollbar_at_bottom = (self.output_text.verticalScrollBar().value() ==
                                   self.output_text.verticalScrollBar().maximum())

            line = line.strip()

            # Preprocess lines
            line_preprocessed = re.sub(r'\W|\d', ' ', line)
            similarities = []

            for previous_line in self.previous_lines:
                previous_line_preprocessed = re.sub(r'\W|\d', ' ', previous_line)
                similarity = jaccard_index(previous_line_preprocessed, line_preprocessed)
                similarities.append(similarity)

            # If the similarity is greater than 0.8 and the line matches the pattern, we consider them as duplicates
            pattern = re.compile(r'(\[.*\]|' + '|'.join(self.keywords) + r'|\d+/\d+|\bcounting\b|\bapplying\b|\%|\|)',
                                 re.IGNORECASE)
            if pattern.search(line):
                if max(similarities, default=0) > 0.8 and self.progress_update:
                    max_index = similarities.index(max(similarities))
                    text = self.output_text.toPlainText()
                    lines = text.split('\n')
                    lines_len = len(lines)

                    # Adjust max_index based on the number of lines in self.output_text and self.previous_lines
                    max_index = lines_len - len(self.previous_lines) + max_index
                    lines = lines[:max_index]

                    # Also adjust previous_lines to mirror the last past_lines lines in output_text
                    self.previous_lines = lines[-self.past_lines:]
                    self.output_text.setPlainText('\n'.join(lines))
                self.progress_update = True
            else:
                self.progress_update = False

            self.output_text.append(line)

            # Add the current line to previous_lines and remove the oldest line if necessary
            self.previous_lines.append(line)
            if len(self.previous_lines) > self.past_lines:
                self.previous_lines.pop(0)

            # Only move the scrollbar to the bottom if it was already there
            if scrollbar_at_bottom:
                self.output_text.verticalScrollBar().setValue(self.output_text.verticalScrollBar().maximum())

        except Exception as e:
            print(f"Error occurred while appending output line: {str(e)}", file=sys.stderr)

    def stop_command(self):
        if self.process:
            self.process.terminate()
            self.worker.output_line.disconnect(self.append_output_line)  # disconnect the signal from the slot
            self.worker = None
            self.process = None
            self.job_stopped = True  # set the flag

            # Add a red "[JOBS STOPPED]" message
            self.output_text.setTextColor(QtGui.QColor("red"))
            self.output_text.append("[JOBS STOPPED]")
            self.output_text.setTextColor(QtGui.QColor("white"))


app = QApplication([])
window = Application(keywords=['Filter', 'rows', 'scores', 'Calculating'], past_lines=5)
window.show()
sys.exit(app.exec_())
