import threading
from dictEditor import LineNumberArea, CodeEditor, DictEditor, JsonHighlighter
from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton, QVBoxLayout, QWidget, QFileDialog, QListWidget, \
    QSplitter, QTextEdit, QLabel, QLineEdit, QHBoxLayout, QStackedWidget, QAction, QComboBox, QScrollArea, \
    QSpinBox, QFormLayout, QDoubleSpinBox, QCheckBox, QGridLayout, QMessageBox, QDialog, QLayout, QDialogButtonBox

from PyQt5.QtCore import Qt, QObject, pyqtSignal, QTimer
from PyQt5 import QtGui
from PyQt5.QtGui import QTextCursor, QColor, QFont, QIcon
import traceback
import sys
import os
import subprocess
import glob
import io
import sys
import re
import json
import inspect
import ast
from jaccard_index.jaccard import jaccard_index

from collections import deque


# Check if the output is redirected
if hasattr(sys.stdout, 'isatty') and not sys.stdout.isatty():
    os.environ["COLUMNS"] = "80"  # Set a default console width

def print_caller_name():
    print(f'Called by: {inspect.stack()[1][3]}')


def convert_numeric_strings(data):
    if isinstance(data, dict):
        return {k: convert_numeric_strings(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_numeric_strings(v) for v in data]
    elif isinstance(data, str):
        try:
            float_val = float(data)
            int_val = int(float_val)
            return int_val if float_val == int_val else float_val
        except ValueError:
            return data
    else:
        return data


def excepthook(type, value, tback):
    traceback.print_exception(type, value, tback)
    sys.__excepthook__(type, value, tback)


sys.excepthook = excepthook


class NoScrollComboBox(QComboBox):
    def wheelEvent(self, event):
        event.ignore()


class CustomDialog(QDialog):
    def __init__(self, title, text, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setStyleSheet("background-color: #2B2B2B; color: #00FFFF; font-family: Courier New; font-size: 11pt;")

        self.text_edit = QTextEdit()
        self.text_edit.setPlainText(text)
        self.text_edit.setReadOnly(True)  # makes the text non-editable

        # Add select all button
        self.select_all_button = QPushButton("Select All")
        self.select_all_button.clicked.connect(self.text_edit.selectAll)

        self.layout = QVBoxLayout()
        self.layout.addWidget(self.text_edit)
        self.layout.addWidget(self.select_all_button)  # add the button to the layout

        self.setLayout(self.layout)
        self.resize(800, 600)  # set an initial size

    def setDictAsPlainText(self, dictionary):
        text = json.dumps(dictionary, indent=4)
        self.text_edit.setPlainText(text)


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


class DictEditorDialog(QDialog):
    def __init__(self, parent, initial_value, schema, options):
        super().__init__(parent)
        self.editor = DictEditor(schema=schema,
                                 data=initial_value,
                                 options=options)
        # Dark theme style for DictEditor
        self.editor.setStyleSheet("""
                    background-color: #2b2b2b;
                    color: #a9b7c6;
                    QLineEdit {
                        background-color: #3c3f41;
                        color: #a9b7c6;
                    }
                    QPushButton {
                        background-color: #3c3f41;
                        color: #a9b7c6;
                    }
                """)

        # Add editor to dialog layout
        layout = QVBoxLayout(self)
        layout.addWidget(self.editor)

    def get_value(self):
        return self.editor.get_value()


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


class JobWidget(QWidget):
    # Define a custom signal to activate when data is changed. This updates underlying jobs data.
    data_changed = pyqtSignal(object)

    def __init__(self, schema, job_number, file_list, parent=None, job_dict=None, verbose=False):
        super().__init__(parent)
        # Set object name for stylesheet targeting
        self.verbose = verbose
        self.setObjectName("jobWidget")

        # Save parent, schema, file list as attributes of the instance
        # -----------------------------------------------------------
        # `parent` is a reference to the parent QWidget (or other QObject) that
        # this QWidget belongs to. In Qt, objects organized in a parent-child
        # hierarchy. This management helps with resource cleanup (when a parent
        # is deleted, all child QObjects are also deleted), and with event handling
        # (events propagate up from child to parent)
        self.parent = parent

        # `schema` is a list of dictionaries, each containing information about a
        # specific job type that can be processed by this widget. The schema describes
        # the parameters each job type needs, their default values, etc. This helps
        # dynamically generate the UI based on the job type selected, and also helps
        # enforce correctness of user input
        self.schema = schema

        # `file_list` is a list of files that are relevant to this widget, which might
        # be required for processing the jobs. This list is used elsewhere in the
        # application where access to the file list is necessary
        self.file_list = file_list

        # `param_line_edits` is a dictionary used to store references to QLineEdit objects
        # created for each parameter of the selected job type. The keys are the names of the
        # parameters (fields), and the values are the QLineEdit objects themselves. Storing
        # these references allows easy access to user input later when the job is run.
        self.param_line_edits = {}

        # Connect to the custom signal
        self.data_changed.connect(parent.update_job_data)

        # `param_labels` is a dictionary used to store QLabel objects created for
        # each parameter of the selected job type and their associated keys from the schema.
        # The keys are the QLabel objects themselves, and the values are the keys from the schema.
        # Storing these references allows easy access to the parameter keys later when the job is run.
        self.param_labels = {}

        # Initialize self.param_content_labels
        self.param_content_labels = {}
        # -----------------------------------------------------------

        # Setting styles for JobWidget and its elements.
        self.setStyleSheet("""
                            JobWidget {
                                border: 2px solid blue;
                            }
                            QLineEdit, QCheckBox {
                                border: 1px solid grey;
                            }
                            QLabel {
                                border: none;
                            }
                            QCheckBox {
                                color: white;
                            }
                        """)

        # Create layout for the widget and set it
        self.job_layout = QGridLayout()
        self.setLayout(self.job_layout)
        self.job_layout.setSpacing(10)

        # Create label for Job Number, set its style and position in the layout
        self.number_label = QLabel(f'Job {job_number}', self)
        self.number_label.setAlignment(Qt.AlignCenter)
        self.number_label.setStyleSheet("font-size: 18px;")
        self.job_layout.addWidget(self.number_label, 0, 0, 3, 1)  # Spanning 3 rows

        # Create Expand/Collapse button, connect its click event to self.toggle method
        self.toggle_button = QPushButton('Collapse', self)
        self.toggle_button.clicked.connect(self.toggle)
        self.toggle_button.setStyleSheet("""
                    QPushButton {
                        min-width: 60px; 
                        min-height: 20px;
                        background: #D3D3D3;
                        color: black;
                    }
                """)
        self.job_layout.addWidget(self.toggle_button, 0, 1, 1, 1)

        # Create Delete button, connect its click event to self.delete method
        self.delete_button = QPushButton('✖', self)
        self.delete_button.setStyleSheet("""
                            QPushButton {
                                color: #c7c1c2; 
                                background-color: #660914; 
                                border: 1px #635a5c; 
                                border-radius: 5px;
                                font-size: 20px;
                            }
                        """)
        self.delete_button.clicked.connect(self.delete)
        self.job_layout.addWidget(self.delete_button, 0, 2)

        # Create label and combo box for Job Type, fill combo box with types from schema
        self.type_label = QLabel('Job Type: ', self)
        self.type_combo_box = NoScrollComboBox(self)
        self.type_combo_box.addItems([job['type'] for job in schema])
        self.type_combo_box.setStyleSheet("color: white;")
        self.type_combo_box.currentTextChanged.connect(self.change_job_type)
        self.job_layout.addWidget(self.type_label, 1, 1)
        self.job_layout.addWidget(self.type_combo_box, 1, 2)

        # Create label and line edit for Job Name, set its default text
        self.name_label = QLabel('Job Name: ', self)
        self.name_line_edit = QLineEdit(self)
        self.name_line_edit.setText(self.schema[0]['default_name'])
        self.job_layout.addWidget(self.name_label, 2, 1)
        self.job_layout.addWidget(self.name_line_edit, 2, 2)

        # Create a widget to hold parameters
        self.params_widget = QWidget()
        self.params_layout = QGridLayout()
        self.params_widget.setLayout(self.params_layout)
        self.params_layout.setHorizontalSpacing(5)
        self.job_layout.addWidget(self.params_widget, 3, 1, 1, 2)

        # Create label and line edit for Output Name (hidden by default)
        self.output_name_label = QLabel('Output Name: ', self)
        self.output_name_label.hide()
        self.output_name_line_edit = QLineEdit(self)
        self.output_name_line_edit.setReadOnly(True)
        self.output_name_line_edit.hide()
        self.job_layout.addWidget(self.output_name_label, 4, 1)
        self.job_layout.addWidget(self.output_name_line_edit, 4, 2)

        self.file_input_layout = None

        # If job_dict is provided, load it into the widget
        if job_dict is not None:
            self.load_job_from_dict(job_dict)
        else:
            # Load parameters based on the job type
            self.change_job_type()

    def load_job_from_dict(self, job_dict):
        # Set job type, which should trigger change_job_type and create the correct input fields
        # self.type_combo_box.setCurrentText(job_dict.get('type', ''))

        # Set the job name
        current_job_name = job_dict.get('name', '')
        if self.verbose:
            sys.stderr.write(f'JOB NAME: {current_job_name}\n')
        self.name_line_edit.setText(current_job_name)

        # Store the job dict for later use in change_job_type
        self.loaded_job_dict = job_dict

        # Change the job type field which will automatically activate the change_job_type function
        self.type_combo_box.setCurrentText(job_dict.get('type', ''))

        current_job_data_to_dict = self.to_dict()
        if self.verbose:
            sys.stderr.write(f'current_job_data_to_dict 1: {current_job_data_to_dict}\n')

    def to_dict2(self):
        # Return a dictionary representation of the job
        job_dict = {
            'type': self.type_combo_box.currentText(),
            'name': self.name_line_edit.text()
        }

        # Iterate through the child widgets of params_widget
        for i in range(self.params_layout.count()):
            layout_item = self.params_layout.itemAt(i)
            widget = layout_item.widget()
            if widget is None:
                # It's a layout
                layout = layout_item.layout()
                # Assuming the QLineEdit is at index 1
                line_edit = layout.itemAt(1).widget()
                if isinstance(line_edit, QLineEdit):
                    # Assuming the QLabel is at index 0
                    label_widget = layout.itemAt(0).widget()
                    label_text = self.param_labels[label_widget]  # Get the key from param_labels
                    line_edit_text = line_edit.text()

                    # Try to parse the line_edit_text back to a dictionary
                    try:
                        line_edit_text = json.loads(line_edit_text)
                    except json.JSONDecodeError:
                        # If the parsing fails, leave it as a string
                        pass

                    job_dict[label_text] = line_edit_text

            # If it's a checkbox, use isChecked method to get the boolean value
            if isinstance(widget, QCheckBox):
                label_text = self.param_labels[widget]
                checkbox_value = widget.isChecked()
                job_dict[label_text] = checkbox_value
            elif isinstance(widget, QLabel):
                # Get the corresponding line edit
                line_edit = self.params_layout.itemAt(i + 1).widget()
                if isinstance(line_edit, QLineEdit):
                    # Extract label text and line edit text
                    label_text = self.param_labels[widget]  # Get the key from param_labels
                    line_edit_text = line_edit.text()

                    # Try to parse the line_edit_text back to a dictionary
                    try:
                        line_edit_text = json.loads(line_edit_text)
                    except json.JSONDecodeError:
                        # If the parsing fails, leave it as a string
                        pass

                    job_dict[label_text] = line_edit_text

        caller_name = inspect.stack()[1][3]  # Get the caller function name
        if self.verbose:
            sys.stderr.write(f'Called by: {caller_name}, JOB DICT: {job_dict}\n')

        return job_dict

    def to_dict3(self):
        # Return a dictionary representation of the job
        job_dict = {
            'type': self.type_combo_box.currentText(),
            'name': self.name_line_edit.text()
        }

        # Get the schema for the current job type
        job_type = self.type_combo_box.currentText()
        job_schema = None
        for job in self.schema:
            if job['type'] == job_type:
                job_schema = job
                break

        # Iterate through the child widgets of params_widget
        for i in range(self.params_layout.count()):
            layout_item = self.params_layout.itemAt(i)
            widget = layout_item.widget()
            if widget is None:
                # It's a layout
                layout = layout_item.layout()
                # Assuming the QLineEdit is at index 1
                line_edit = layout.itemAt(1).widget()
                if isinstance(line_edit, QLineEdit):
                    # Assuming the QLabel is at index 0
                    label_widget = layout.itemAt(0).widget()
                    label_text = self.param_labels[label_widget]  # Get the key from param_labels
                    line_edit_text = line_edit.text()

                    # Use the schema to interpret the line_edit_text
                    param_type = job_schema['params'][label_text]['type']
                    if param_type == 'list':
                        try:
                            line_edit_text = ast.literal_eval(line_edit_text) if line_edit_text else []
                        except (ValueError, SyntaxError):
                            pass  # If it fails, leave it as is (it might be a list of strings)
                    elif param_type == 'dict':
                        try:
                            line_edit_text = ast.literal_eval(line_edit_text) if line_edit_text else {}
                        except (ValueError, SyntaxError):
                            pass  # If it fails, leave it as is (it might be a string representation of a dict)

                    job_dict[label_text] = line_edit_text

            # If it's a checkbox, use isChecked method to get the boolean value
            if isinstance(widget, QCheckBox):
                label_text = self.param_labels[widget]
                checkbox_value = widget.isChecked()
                job_dict[label_text] = checkbox_value
            elif isinstance(widget, QLabel):
                # Get the corresponding line edit
                line_edit = self.params_layout.itemAt(i + 1).widget()
                if isinstance(line_edit, QLineEdit):
                    # Extract label text and line edit text
                    label_text = self.param_labels[widget]  # Get the key from param_labels
                    line_edit_text = line_edit.text()

                    # Use the schema to interpret the line_edit_text
                    param_type = job_schema['params'][label_text]['type']
                    if param_type == 'list':
                        try:
                            line_edit_text = ast.literal_eval(line_edit_text) if line_edit_text else []
                        except (ValueError, SyntaxError):
                            pass  # If it fails, leave it as is (it might be a list of strings)
                    elif param_type == 'dict':
                        try:
                            line_edit_text = ast.literal_eval(line_edit_text) if line_edit_text else {}
                        except (ValueError, SyntaxError):
                            pass  # If it fails, leave it as is (it might be a string representation of a dict)

                    job_dict[label_text] = line_edit_text

        return job_dict

    def to_dict4(self):
        # Return a dictionary representation of the job
        job_dict = {
            'type': self.type_combo_box.currentText(),
            'name': self.name_line_edit.text()
        }

        # Get the schema for the current job type
        job_type = self.type_combo_box.currentText()
        job_schema = None
        for job in self.schema:
            if job['type'] == job_type:
                job_schema = job
                break

        # Iterate through the child widgets of params_widget
        for i in range(self.params_layout.count()):
            layout_item = self.params_layout.itemAt(i)
            widget = layout_item.widget()
            if widget is None:
                # It's a layout
                layout = layout_item.layout()
                # Assuming the QLineEdit is at index 1
                line_edit = layout.itemAt(1).widget()
                if isinstance(line_edit, QLineEdit):
                    # Assuming the QLabel is at index 0
                    label_widget = layout.itemAt(0).widget()
                    label_text = self.param_labels[label_widget]  # Get the key from param_labels
                    line_edit_text = line_edit.text()

                    # Use the schema to interpret the line_edit_text
                    param_type = job_schema['params'][label_text]['type']
                    if param_type == 'list':
                        try:
                            line_edit_text = ast.literal_eval(line_edit_text) if line_edit_text else []
                        except (ValueError, SyntaxError):
                            pass  # If it fails, leave it as is (it might be a list of strings)
                    elif param_type == 'dict':
                        try:
                            line_edit_text = ast.literal_eval(line_edit_text) if line_edit_text else {}
                        except (ValueError, SyntaxError):
                            pass  # If it fails, leave it as is (it might be a string representation of a dict)
                    elif param_type in ['int', 'float', 'num', 'number']:
                        try:
                            line_edit_text = float(line_edit_text) if line_edit_text else None
                            if param_type == 'int':
                                line_edit_text = int(line_edit_text) if line_edit_text is not None else None
                        except ValueError:
                            pass  # If conversion fails, leave it as is (it might be a string representation)

                    job_dict[label_text] = line_edit_text

            # If it's a checkbox, use isChecked method to get the boolean value
            if isinstance(widget, QCheckBox):
                label_text = self.param_labels[widget]
                checkbox_value = widget.isChecked()
                job_dict[label_text] = checkbox_value

        return job_dict

    def to_dict5(self):
        # Return a dictionary representation of the job
        job_dict = {
            'type': self.type_combo_box.currentText(),
            'name': self.name_line_edit.text()
        }

        # Get the schema for the current job type
        job_type = self.type_combo_box.currentText()
        job_schema = None
        for job in self.schema:
            if job['type'] == job_type:
                job_schema = job
                break
        # Iterate through the child widgets of params_widget
        for i in range(self.params_layout.count()):
            layout_item = self.params_layout.itemAt(i)
            widget = layout_item.widget()
            if widget is None:
                # It's a layout
                layout = layout_item.layout()
                # Assuming the QLineEdit is at index 1
                line_edit = layout.itemAt(1).widget()
                if isinstance(line_edit, QLineEdit):
                    # Assuming the QLabel is at index 0
                    label_widget = layout.itemAt(0).widget()
                    label_text = self.param_labels[label_widget]  # Get the key from param_labels
                    line_edit_text = line_edit.text()

                    # Use the schema to interpret the line_edit_text
                    param_type = job_schema['params'][label_text]['type']
                    if param_type == 'list':
                        try:
                            line_edit_text = ast.literal_eval(line_edit_text) if line_edit_text else []
                        except (ValueError, SyntaxError):
                            pass  # If it fails, leave it as is (it might be a list of strings)
                    elif param_type == 'dict':
                        try:
                            line_edit_text = ast.literal_eval(line_edit_text) if line_edit_text else {}
                        except (ValueError, SyntaxError):
                            pass  # If it fails, leave it as is (it might be a string representation of a dict)
                    elif param_type in ['int', 'float', 'num', 'number']:
                        try:
                            if line_edit_text:  # Only convert if there's something to convert
                                line_edit_text = float(line_edit_text)
                                if param_type == 'int':
                                    line_edit_text = int(line_edit_text)
                            else:
                                line_edit_text = None  # Or '' if you want it to be an empty string
                        except ValueError:
                            pass  # If conversion fails, leave it as is (it might be a string representation)

                    job_dict[label_text] = line_edit_text

            elif isinstance(widget, QLineEdit):
                # Get the corresponding QLabel
                label_widget = self.params_layout.itemAt(i - 1).widget()
                if isinstance(label_widget, QLabel):
                    # Extract label text and line edit text
                    label_text = self.param_labels[label_widget]  # Get the key from param_labels
                    line_edit_text = widget.text()  # Now widget is the QLineEdit

            # If it's a checkbox, use isChecked method to get the boolean value
            if isinstance(widget, QCheckBox):
                label_text = self.param_labels[widget]
                checkbox_value = widget.isChecked()
                job_dict[label_text] = checkbox_value

        return job_dict

    # TODO finalize processing of lists and numbers
    def to_dict(self):
        # Return a dictionary representation of the job
        job_dict = {
            'type': self.type_combo_box.currentText(),
            'name': self.name_line_edit.text()
        }

        # Get the schema for the current job type
        job_type = self.type_combo_box.currentText()
        job_schema = None
        for job in self.schema:
            if job['type'] == job_type:
                job_schema = job
                break

        # Iterate through the child widgets of params_widget
        for i in range(self.params_layout.count()):
            layout_item = self.params_layout.itemAt(i)
            widget = layout_item.widget()
            if widget is None:
                # It's a layout
                layout = layout_item.layout()
                # Assuming the QLineEdit is at index 1
                line_edit = layout.itemAt(1).widget()
                if isinstance(line_edit, QLineEdit):
                    # Assuming the QLabel is at index 0
                    label_widget = layout.itemAt(0).widget()
                    label_text = self.param_labels[label_widget]  # Get the key from param_labels
                    line_edit_text = line_edit.text()
                    param_type = job_schema['params'][label_text]['type']
                    if param_type == 'dict':
                        try:
                            if self.verbose:
                                sys.stderr.write(f'{ast.literal_eval(line_edit_text)}\n')
                            line_edit_text = ast.literal_eval(line_edit_text) if line_edit_text else {}
                        except (ValueError, SyntaxError):
                            if self.verbose:
                                sys.stderr.write(f'DICT SYNTAX VALUE ERROR\n')
                            pass  # If it fails, leave it as is (it might be a string representation of a dict)
                    job_dict[label_text] = line_edit_text

            # If it's a checkbox, use isChecked method to get the boolean value
            if isinstance(widget, QCheckBox):
                label_text = self.param_labels[widget]
                checkbox_value = widget.isChecked()
                job_dict[label_text] = checkbox_value
            elif isinstance(widget, QLabel):
                # Get the corresponding line edit
                line_edit = self.params_layout.itemAt(i + 1).widget()
                if isinstance(line_edit, QLineEdit):
                    # Extract label text and line edit text
                    label_text = self.param_labels[widget]  # Get the key from param_labels
                    line_edit_text = line_edit.text()

                    # Use the schema to interpret the line_edit_text
                    param_type = job_schema['params'][label_text]['type']
                    # if param_type == 'list':
                    #     sys.stderr.write(f'QLabel LIST\n')
                    #     # try:
                    #     #     line_edit_text = ast.literal_eval(line_edit_text) if line_edit_text else []
                    #     # except (ValueError, SyntaxError):
                    #     #     pass  # If it fails, leave it as is (it might be a list of strings)
                    #     try:
                    #         line_edit_text = json.loads(line_edit_text) if line_edit_text else []
                    #     except (json.JSONDecodeError, ValueError):
                    #         pass  # If it fails, leave it as is (it might be a list of strings)
                    if param_type == 'list':
                        # Try to interpret it as a python expression first (it handles lists of mixed types and nested lists)
                        try:
                            line_edit_text = ast.literal_eval(line_edit_text)
                        except (ValueError, SyntaxError):
                            # If that fails, treat it as a comma-separated list
                            line_edit_text = line_edit_text.split(',')
                            for i, item in enumerate(line_edit_text):
                                item = item.strip()  # Remove leading/trailing whitespace
                                # Try to convert to int or float if possible
                                try:
                                    line_edit_text[i] = int(item)
                                except ValueError:
                                    try:
                                        line_edit_text[i] = float(item)
                                    except ValueError:
                                        # If it's not an int or a float, assume it's a string and remove quotes if present
                                        line_edit_text[i] = item.strip('"').strip("'")
                        job_dict[label_text] = line_edit_text
                    elif param_type == 'dict':
                        try:
                            line_edit_text = ast.literal_eval(line_edit_text) if line_edit_text else {}
                        except (ValueError, SyntaxError):
                            pass  # If it fails, leave it as is (it might be a string representation of a dict)
                    elif param_type in ['int', 'float', 'num', 'number']:
                        try:
                            if line_edit_text:  # Only convert if there's something to convert
                                line_edit_text = float(line_edit_text)
                                if param_type == 'int':
                                    line_edit_text = int(line_edit_text)
                            else:
                                # line_edit_text = None  # Or '' if you want it to be an empty string
                                line_edit_text = ""
                        except ValueError:
                            pass  # If conversion fails, leave it as is (it might be a string representation)

                    job_dict[label_text] = line_edit_text

        caller_name = inspect.stack()[1][3]  # Get the caller function name
        if self.verbose:
            sys.stderr.write(f'Called by: {caller_name}, JOB DICT: {job_dict}\n')

        return job_dict

    def create_param_input(self, param_name, param_config):
        if param_config['type'] == 'file':
            # Create layout for file input
            file_input_layout = QHBoxLayout()

            # Create QLabel for parameter name
            param_label = QLabel(f"{param_name}: ", self)
            param_label.setStyleSheet("color: white;")
            self.param_labels[param_label] = param_name

            # Create QLineEdit for file name
            file_name_input = QLineEdit(self)
            self.param_line_edits[param_name] = file_name_input
            file_name_input.textChanged.connect(self.update_output)
            file_name_input.textChanged.connect(self.update_data)

            # Create QPushButton for file selection
            file_selection_button = QPushButton("Select File", self)
            file_selection_button.clicked.connect(self.file_dialog)

            # Add widgets to the layout
            file_input_layout.addWidget(param_label)
            file_input_layout.addWidget(file_name_input)
            file_input_layout.addWidget(file_selection_button)

            # Return the layout
            return file_input_layout

        elif param_config['type'] == 'dict':
            # Create layout for dict input
            dict_input_layout = QHBoxLayout()

            # Create QLabel for parameter name
            param_label = QLabel(f"{param_name}: ", self)
            param_label.setStyleSheet("color: white;")
            self.param_labels[param_label] = param_name

            # Create QLineEdit for dict content
            default_dict_content = ""
            try:
                default_dict_content = json.dumps(param_config['default'])
            except Exception as e:
                pass

            dict_content_label = QLineEdit(default_dict_content, self)
            dict_content_label.setReadOnly(True)
            dict_content_label.setStyleSheet("color: white;")  # Style as needed
            self.param_content_labels[param_name] = dict_content_label

            # Create QPushButton for dict editing
            dict_edit_button = QPushButton("Edit Object", self)
            dict_edit_button.clicked.connect(lambda: self.open_dict_editor(param_name, param_config))

            # Add widgets to the layout
            dict_input_layout.addWidget(param_label)
            dict_input_layout.addWidget(dict_content_label)
            dict_input_layout.addWidget(dict_edit_button)

            # Return the layout
            return dict_input_layout

    def open_dict_editor(self, param_name, param_config):
        # Check if param_line_edits already has a value for this parameter
        if param_name in self.param_line_edits and self.param_line_edits[param_name] != "":
            initial_value = self.param_line_edits[param_name]
        else:
            initial_value = param_config.get('default', None)  # Use default value here

        schema = param_config.get('schema', {})  # Get schema if it exists else use an empty dict
        options = param_config.get('options', {})  # Get options if it exists else use an empty dict

        # Create dict editor dialog with the schema and initial value
        editor_dialog = DictEditorDialog(self,
                                         initial_value=initial_value,
                                         schema=schema,
                                         options=options)
        # TODO make sure finished only works with sucessful saves
        editor_dialog.finished.connect(lambda: self.update_param_line_edits_and_labels(param_name, editor_dialog))
        editor_dialog.show()

    def update_param_input(self, param_name, text):
        output_prepend = self.current_job_in_schema.get('output_prepends', '')
        output_text = f"{output_prepend}{os.path.basename(text)}"
        self.param_line_edits[param_name].setText(output_text)

    def update_param_line_edits_and_labels(self, param_name, editor_dialog):
        new_value = editor_dialog.get_value()
        self.param_line_edits[param_name] = new_value
        if self.verbose:
            sys.stderr.write(f'update_param_line_edits_and_labels -- {param_name} == {new_value}\n')

        # Update dict_content_label with a minified version of the new_value
        self.param_content_labels[param_name].setText(json.dumps(new_value, separators=(',', ':')))

        self.update_data()  # makes sure that the dictionary data is added to the output

    def change_job_type(self):
        # Check if self.loaded_job_dict exists and save it
        loaded_job_dict = None
        if hasattr(self, 'loaded_job_dict'):
            loaded_job_dict = self.loaded_job_dict
            del self.loaded_job_dict

        caller_name = inspect.stack()[1][3]
        if self.verbose:
            sys.stderr.write(f'Called by: {caller_name}, LOADED_JOB_DICT: {loaded_job_dict}\n')

        # Clear previous job parameters
        self.clear_layout(self.params_layout)
        self.param_line_edits = {}
        self.output_file = None
        self.output_name_line_edit.clear()
        self.output_name_line_edit.hide()
        self.output_name_label.hide()
        self.param_labels = {}  # Reset the param_labels dictionary

        # Create a dictionary to store the widgets for each parameter
        self.param_widgets = {}

        # Get information about the selected job type to generate input boxes and labels
        for job in self.schema:
            if job['type'] == self.type_combo_box.currentText():
                self.current_job_in_schema = job
                if not self.name_line_edit.text():
                    self.name_line_edit.setText(job['default_name'])
                row_index = 0
                for param, config in job['params'].items():
                    # Create parameter widgets based on their type
                    if config['type'] == 'bool':
                        checkbox = QCheckBox(f'{param}: ', self)
                        checkbox.setStyleSheet("color: white;")
                        self.params_layout.addWidget(checkbox, row_index, 0, 1, 2)
                        checkbox.stateChanged.connect(self.update_data)
                        self.param_labels[checkbox] = param  # Add QCheckBox and its param to param_labels
                        self.param_widgets[param] = checkbox
                        if loaded_job_dict and param in loaded_job_dict:
                            checkbox.setChecked(loaded_job_dict[param])
                    elif config['type'] == 'file':
                        param_input_layout = self.create_param_input(param, config)
                        self.params_layout.addLayout(param_input_layout, row_index, 0, 1, 2)
                        self.param_widgets[param] = param_input_layout  # Add QLayout to param_widgets
                        if loaded_job_dict and param in loaded_job_dict:
                            self.param_line_edits[param].setText(loaded_job_dict[param])
                    elif config['type'] == 'dict':
                        param_input_layout = self.create_param_input(param, config)
                        self.params_layout.addLayout(param_input_layout, row_index, 0, 1, 2)
                        self.param_widgets[param] = param_input_layout  # Add QLayout to param_widgets
                        if loaded_job_dict and param in loaded_job_dict:
                            dict_value = loaded_job_dict[param]
                            self.param_content_labels[param].setText(json.dumps(dict_value, separators=(',', ':')))
                            self.param_line_edits[param] = dict_value
                    elif config['type'] == 'list':
                        label = QLabel(f'{param}: ', self)
                        label.setStyleSheet("color: white;")
                        self.param_labels[label] = param
                        line_edit = QLineEdit(self)
                        self.params_layout.addWidget(label, row_index, 0)
                        self.params_layout.addWidget(line_edit, row_index, 1)
                        line_edit.textChanged.connect(self.update_data)
                        self.param_widgets[param] = line_edit
                        if loaded_job_dict and param in loaded_job_dict:
                            list_value = loaded_job_dict[param]
                            # Convert list to string representation with double quotes
                            list_str = json.dumps(list_value)
                            line_edit.setText(list_str)
                    else:
                        label = QLabel(f'{param}: ', self)
                        label.setStyleSheet("color: white;")
                        self.param_labels[label] = param
                        line_edit = QLineEdit(self)
                        self.params_layout.addWidget(label, row_index, 0)
                        self.params_layout.addWidget(line_edit, row_index, 1)
                        line_edit.textChanged.connect(self.update_data)
                        self.param_widgets[param] = line_edit
                        if loaded_job_dict and param in loaded_job_dict:
                            line_edit.setText(str(loaded_job_dict[param]))
                    row_index += 1

                # Show output name if necessary fields exist in job
                if all(key in self.current_job_in_schema for key in ['input_param', 'output_prepends', 'output_ext']):
                    self.output_name_line_edit.show()
                    self.output_name_label.show()

    def job_type_changed(self, job_type):
        # Get job schema
        job_schema = next((job for job in self.schema if job['type'] == job_type), None)
        if not job_schema:
            return

        # set the default_name line to the one associated with the jbo type form the schema file
        self.name_line_edit.setText(job_schema['default_name'])

        # Clear previous job parameters
        for parameter_widget in self.job_parameters.values():
            self.layout.removeRow(parameter_widget)
        self.job_parameters.clear()

        # Add new job parameters
        for parameter, parameter_info in job_schema['params'].items():
            # Based on the parameter type, different input widgets can be added
            if parameter_info['type'] == 'file':
                parameter_widget = QLineEdit()
            elif parameter_info['type'] == 'int':
                parameter_widget = QSpinBox()
            elif parameter_info['type'] == 'float':
                parameter_widget = QDoubleSpinBox()
            elif parameter_info['type'] == 'bool':
                parameter_widget = QComboBox()
                parameter_widget.addItems(parameter_info['options'])
            elif parameter_info['type'] == 'dict':
                parameter_widget = QTextEdit()
            else:
                parameter_widget = QLineEdit()

            self.job_parameters[parameter] = parameter_widget
            self.layout.addRow(QLabel(parameter.capitalize() + ":"), parameter_widget)
        self.update_data()

    def toggle(self):
        for i in range(self.params_layout.count()):
            widget = self.params_layout.itemAt(i).widget()
            if widget:
                widget.setVisible(not widget.isVisible())
        self.toggle_button.setText('Expand' if self.toggle_button.text() == 'Collapse' else 'Collapse')

    def clear_layout(self, layout):
        while layout.count():
            child = layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
            elif child.layout():
                self.clear_layout(child.layout())

    def update_output(self):
        if self.current_job_in_schema['input_param'] in self.param_line_edits:
            input_file = self.param_line_edits[self.current_job_in_schema['input_param']].text()
            base_input_file, _ = os.path.splitext(os.path.basename(input_file))
            self.output_file = \
                f"{self.current_job_in_schema['output_prepends']}{base_input_file}.{self.current_job_in_schema['output_ext']}"
            self.output_name_line_edit.setText(self.output_file)  # Update the output name display

    def file_dialog(self):
        fname = QFileDialog.getOpenFileName(self, 'Select Input File', filter='All Files (*)')[0]
        if fname:
            base_fname = os.path.basename(fname)
            self.param_line_edits[self.current_job_in_schema['input_param']].setText(base_fname)

    def update_job_numbers(self):
        for i, job in enumerate(self.jobs, start=1):
            job.number_label.setText(f'Job {i}: ')

    def delete(self):
        self.parent.jobs.remove(self)  # Remove this job from the jobs list
        self.parent.update_job_numbers()  # Update job numbers
        self.deleteLater()

    # This function should be called whenever the data of the job changes
    def update_data(self):
        # ... Update the data ...
        # Emit the signal
        self.data_changed.emit(self)


class BuilderWidget(QWidget):
    def __init__(self, exe_mode=False):
        # Call to the parent constructor
        super().__init__()

        self.verbose = True if exe_mode else False

        # Create a QVBoxLayout (Vertical Box Layout) and set it as the layout of the current QWidget
        self.main_layout = QVBoxLayout()
        self.setLayout(self.main_layout)

        # File selector
        # `file_line_edit` QLineEdit object holds the path of the selected JSON file
        # It's updated when user selects a file from the dropdown or using the file dialog
        self.file_label = QLabel("JSON Config File:", self)
        self.file_line_edit = QLineEdit(self)
        self.file_label.setBuddy(self.file_line_edit)  # Associate the label with the QLineEdit

        # Button to open the file dialog
        self.file_button = QPushButton("Select JSON Config", self)
        self.file_button.clicked.connect(self.file_dialog)

        # Dropdown list to select a JSON file
        self.file_dropdown = QComboBox(self)
        self.file_dropdown.addItems([f for f in os.listdir() if re.match(r'^config.*\.json$', f)])
        self.file_dropdown.currentTextChanged.connect(self.file_line_edit.setText)

        # Layout for the file button and dropdown
        file_select_layout = QHBoxLayout()
        file_select_layout.addWidget(self.file_button)
        file_select_layout.addWidget(self.file_dropdown)

        # Button to load jobs from the selected file
        self.load_button = QPushButton("Load JSON", self)
        self.load_button.setStyleSheet("color: #FFFFFF; background-color: #2673d1;")
        self.load_button.clicked.connect(lambda: self.load_jobs_from_file())

        # Layout for the file line edit and load button
        file_load_layout = QHBoxLayout()
        file_load_layout.addWidget(self.file_label)
        file_load_layout.addWidget(self.file_line_edit)
        file_load_layout.addWidget(self.load_button)

        # Add file_select_layout and file_load_layout to the main layout
        self.main_layout.addLayout(file_select_layout)
        self.main_layout.addLayout(file_load_layout)

        # Load schema
        # `schema` is loaded from the 'job_schema.json' file. It's a list of dictionaries
        # which define the properties and parameters of the different types of jobs
        try:
            with open('job_schema.json') as f:
                try:
                    self.schema = json.load(f)['job_schema']
                except Exception as e:
                    if self.verbose:
                        self.showError(f'Json Schema file error: {e}')
                    exit(1)
        except FileNotFoundError:
            self.showError("Error: 'job_schema.json' file not found.")
            return  # Stop the initialization
        except json.JSONDecodeError:
            self.showError("Error: Invalid JSON format in 'job_schema.json'.")
            return  # Stop the initialization
        except Exception as e:
            if self.verbose:
                self.showError(f'Json Schema file error: {e}')
            exit(1)
        # `jobs` is a list to hold references to JobWidgets. Each JobWidget represents a job in the UI
        self.jobs = []

        # jobs_data is a list to hold underlying data of all the jobs widgets
        self._jobs_data = []  # Initialize _jobs_data first
        self.jobs_data = []  #

        # Initialize self.updating to False
        self.updating = False

        # Create a QVBoxLayout for the jobs and add it to the main layout
        self.jobs_layout = QVBoxLayout()
        self.jobs_layout.setSpacing(0)  # Adding some spacing between jobs for better readability

        # Button to add a new job
        self.add_job_button = QPushButton("+ Add Job +", self)
        self.add_job_button.clicked.connect(lambda: self.add_job())
        self.main_layout.addWidget(self.add_job_button)

        # Create a QScrollArea to allow scrolling through the jobs
        self.jobs_scroll_area = QScrollArea()
        self.jobs_scroll_area.setWidgetResizable(True)

        # Create a QVBoxLayout for the jobs_widget which will hold all the jobs
        self.jobs_layout = QVBoxLayout()
        self.jobs_layout.setAlignment(Qt.AlignTop)  # Align child widgets to the top

        # Create a QWidget to add to the QScrollArea. All jobs are added to this widget
        self.jobs_widget = QWidget()
        self.jobs_widget.setStyleSheet("background: transparent;")
        self.jobs_widget.setLayout(self.jobs_layout)

        # Set the jobs_widget as the widget for the jobs_scroll_area
        self.jobs_scroll_area.setWidget(self.jobs_widget)
        self.jobs_scroll_area.setStyleSheet(
            "QScrollArea {border: 1px solid white; border-radius: 5px; background: transparent;}")
        self.main_layout.addWidget(self.jobs_scroll_area)

        # `file_list` is a list to hold references to the files used in the jobs
        # It's initially empty, and gets populated as jobs are added/loaded
        self.file_list = []

        # Save buttons
        self.save_buttons_grid = QHBoxLayout()

        # When clicked, it triggers the save_config function which saves the current state of the jobs to a JSON file
        self.save_button = QPushButton("Save Config", self)
        self.save_button.setStyleSheet("color: #FFFFFF; background-color: #008000;")
        self.save_button.clicked.connect(lambda: self.save_config())
        self.save_buttons_grid.addWidget(self.save_button)

        # Add after the existing save_button
        self.save_as_button = QPushButton("Save Config As", self)
        self.save_as_button.setStyleSheet("color: #FFFFFF; background-color: #4a81d9;")  # Dark blue color
        self.save_as_button.clicked.connect(self.save_config_as)
        self.save_buttons_grid.addWidget(self.save_as_button)

        self.main_layout.addLayout(self.save_buttons_grid)

        # Add a "Show Jobs Data" button to the UI
        self.show_jobs_data_button = QPushButton("Show Outputted Config JSON", self)
        self.show_jobs_data_button.clicked.connect(self.show_jobs_data)
        self.main_layout.addWidget(self.show_jobs_data_button)

        # Create success message label
        self.success_message = QLabel(self.jobs_widget)
        self.success_message.setFixedSize(300, 100)
        self.success_message.setStyleSheet("""
                    background-color: #008000;
                    color: #FFFFFF;
                    font-weight: bold;
                    border-radius: 5px;
                    qproperty-alignment: AlignCenter;
                """)
        self.success_message.hide()  # Initially hidden

    @property
    def jobs_data(self):
        return self._jobs_data

    @jobs_data.setter
    def jobs_data(self, jobs_data):
        # Check if jobs_data is actually changed
        try:
            if jobs_data == self._jobs_data:
                return
        except BaseException:
            return

        self._jobs_data = jobs_data
        # Load jobs from the data
        # self.load_jobs(jobs_data)

    def scroll_to_bottom(self):
        self.jobs_scroll_area.verticalScrollBar().setValue(self.jobs_scroll_area.verticalScrollBar().maximum())

    def file_dialog(self):
        fname = QFileDialog.getOpenFileName(self, 'Select JSON Config', filter='JSON Files (*.json)')[0]
        if fname:
            self.file_line_edit.setText(fname)

    def update_job_numbers(self):
        for i, job in enumerate(self.jobs, start=1):
            job.number_label.setText(f"Job {i}: ")

    def delete_job(self, job_widget):
        # Remove the job widget from the list and update job numbers
        self.jobs.remove(job_widget)
        self.update_job_numbers()

        # Remove the job widget from layout and delete it
        self.jobs_layout.removeWidget(job_widget)
        job_widget.deleteLater()

    def load_jobs_from_file(self):
        file_name = self.file_line_edit.text()
        if not file_name:
            QMessageBox.warning(self, 'Warning', 'No file name specified.')
            return

        try:
            with open(file_name, 'r') as file:
                config = json.load(file)
                new_jobs_data = config.get('jobs', [])
        except FileNotFoundError:
            QMessageBox.warning(self, 'Warning', f'File not found: {file_name}')
            return
        except json.JSONDecodeError:
            QMessageBox.warning(self, 'Warning', f'Invalid JSON format in file: {file_name}')
            return

        # Remove all existing jobs before loading the new jobs
        while self.jobs:
            job_widget = self.jobs[-1]
            self.delete_job(job_widget)

        # Load the new jobs from the loaded data
        for job_dict in new_jobs_data:
            caller_name = inspect.stack()[1][3]
            if self.verbose:
                sys.stderr.write(f'Called by: {caller_name}, ADD DICT: {job_dict}\n')
            self.add_job(job_dict)

    # This function adds a job, and connects its signals to appropriate slots
    def add_job(self, job_dict=None):
        # This function accepts an optional argument job_dict, which is a dictionary
        # that represents a job's data. If job_dict is provided, a new job will be created
        # using this data. If it's not provided (None), a new empty job will be created.

        if not isinstance(job_dict, dict) and job_dict is not None:
            # This is a type check for job_dict. It raises an error if job_dict is neither
            # a dictionary nor None.
            raise TypeError('job_dict should be a dictionary or None')

        # Set self.updating to True before creating a JobWidget. This variable is used to
        # prevent the jobs_data setter from being triggered when we're updating jobs_data ourselves.
        self.updating = True

        # Create a new JobWidget. If job_dict is provided, the new job will be initialized with
        # this data. The job's number is the current number of jobs plus 1. The parent of the job
        # widget is this BuilderWidget, and the schema and file list are shared among all jobs.
        if self.verbose:
            sys.stderr.write(f'job dict (add_job): {job_dict}\n')
        job_widget = JobWidget(schema=self.schema,
                               job_number=len(self.jobs) + 1,
                               file_list=self.file_list,
                               parent=self,
                               job_dict=job_dict)
        if self.verbose:
            sys.stderr.write(f'JOB WIDGET to_dict: {job_widget.to_dict()}\n')

        # Add the new job widget to the jobs layout, and add it to the list of jobs.
        self.jobs_layout.addWidget(job_widget)
        self.jobs.append(job_widget)

        # Update jobs_data to reflect the newly added job. This step is necessary because
        # the new job's data is not in jobs_data yet. self.jobs (the above list) is used to do this.
        self.update_jobs_data_from_jobs()

        # Set self.updating to False after the JobWidget is added to the jobs list.
        # Now, if a job's data is changed, the jobs_data setter can be triggered to update
        # jobs_data accordingly.
        self.updating = False

        # Connect the job widget's data_changed signal to the update_job_data slot.
        # This means when a job's data is changed, the update_job_data function will be called,
        # which updates the corresponding data in jobs_data.
        job_widget.data_changed.connect(lambda: self.update_job_data(job_widget))  # TODO

        # Update the job numbers displayed on the job widgets. This is necessary because
        # the job number is based on its order in the jobs list, which might have changed.
        self.update_job_numbers()

        # Scroll the scroll area to the bottom, so the newly added job is visible.
        self.scroll_to_bottom()

    def update_jobs_data_from_jobs(self):
        # Loop through each job widget and extract its underlying data using to_dict() function
        caller_name = inspect.stack()[1][3]
        if self.verbose:
            sys.stderr.write(f'Called by: {caller_name}, before UPDATE self.jobs_data: {self.jobs_data}\n')
        self.jobs_data = [job.to_dict() for job in self.jobs]  # set underlying jobs_data
        if self.verbose:
            sys.stderr.write(f'Called by: {caller_name}, after UPDATE self.jobs_data: {self.jobs_data}\n')

    # This function will update the job data in the list when a job's data changes
    def update_job_data(self, job_widget):
        if self.updating:
            return
        job_index = self.jobs.index(job_widget)
        job_data = job_widget.to_dict()
        if self.verbose:
            sys.stderr.write(f'UPDATE JSON (to_dict): {job_data}\n')
        new_jobs_data = self.jobs_data[:]
        new_jobs_data[job_index] = job_data
        self.jobs_data = new_jobs_data  # directly set jobs_data

    def show_jobs_data(self):
        # Create a dictionary with a jobs key, but don't alter self.jobs_data
        jobs_data_dict = {'jobs': convert_numeric_strings(self.jobs_data)}
        if self.verbose:
            sys.stderr.write(f'SHOW JSON: {jobs_data_dict}\n')
        # Show the dictionary in a custom message box
        dialog = CustomDialog('Current Config JSON', '', self)
        dialog.setDictAsPlainText(jobs_data_dict)
        dialog.exec_()

    def show_success_message(self):
        # Calculate position to center the message at the bottom
        x_position = int((self.jobs_widget.width() - self.success_message.width()) / 2)
        y_position = int(self.jobs_widget.height() - self.success_message.height() - 20)  # 20 for margin

        # Position the message
        self.success_message.move(x_position, y_position)

        self.success_message.setText("Configuration successfully saved.")
        self.success_message.show()  # Show the message

        # Create a QTimer instance and connect its timeout signal to the hide method of success_message
        QTimer.singleShot(3000, self.success_message.hide)  # Hide the message after 3 seconds

    def save_config(self):
        file_name = self.file_line_edit.text()
        if not file_name:
            QMessageBox.warning(self, 'Warning', 'No file name specified.')
            return

        # Create a dictionary with a jobs key, but don't alter self.jobs_data
        jobs_data_dict = {'jobs': convert_numeric_strings(self.jobs_data)}

        try:
            with open(file_name, 'w') as file:
                json.dump(jobs_data_dict, file, indent=4)
            self.show_success_message()
        except Exception as e:
            QMessageBox.warning(self, 'Warning', f'Failed to write to file: {file_name}\nError: {e}')
            if self.verbose:
                sys.stderr.write(f'{e}')

    # Save Config As function
    def save_config_as(self):
        options = QFileDialog.Options()
        file_name, _ = QFileDialog.getSaveFileName(self, "QFileDialog.getSaveFileName()", "",
                                                   "JSON Files (*.json);;All Files (*)", options=options)
        if file_name:
            if not file_name.endswith('.json'):  # If user did not include .json, add it
                file_name += '.json'
            # Create a dictionary with a jobs key, but don't alter self.jobs_data
            jobs_data_dict = {'jobs': convert_numeric_strings(self.jobs_data)}
            try:
                with open(file_name, 'w') as file:
                    json.dump(jobs_data_dict, file, indent=4)
                self.show_success_message()
            except Exception as e:
                QMessageBox.warning(self, 'Warning', f'Failed to write to file: {file_name}\nError: {e}')
                if self.verbose:
                    sys.stderr.write(f'{e}')

    def showError(self, message):
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Critical)
        msg.setText(message)
        msg.setWindowTitle("Error")
        msg.exec_()

class RunWidget(QWidget):
    def __init__(self, keywords=None, past_lines=3):
        super().__init__()
        self.layout = QVBoxLayout(self)
        self.setLayout(self.layout)  # set the layout of RunWidget

        self.output_text = TerminalEmulator()
        self.emitting_stream = EmittingStream(self)
        self.emitting_stream.textWritten.connect(self.output_text.write)
        sys.stdout = self.emitting_stream

        self.worker = None
        self.past_lines = past_lines
        self.keywords = keywords if keywords else []
        self.previous_lines = []  # List to store past lines

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
            self.showError("Invalid interpreter!")
            return
        if not os.path.isfile(selected_file) or not selected_file.lower().endswith(".json"):
            self.showError("Invalid file!")
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
            self.showError(f"Error occurred while appending output line: {str(e)}")

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

    def showError(self, message):
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Critical)
        msg.setText(message)
        msg.setWindowTitle("Error")
        msg.exec_()

class Application(QMainWindow):
    def __init__(self, exe_mode=False):
        super().__init__()

        # Set window icon
        self.setWindowIcon(QIcon('jsonaut.ico'))  # Provide correct path to your .ico file

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
        self.setWindowTitle("Jsonaut")

        # Create an instance of EmittingStream and hold onto it
        self.emitting_stream = EmittingStream(self)

        # Create the Run and Builder widgets and add them to the stacked widget
        self.run_widget = RunWidget(keywords=['Filter', 'rows', 'scores', 'Calculating'],
                                    past_lines=5)
        self.builder_widget = BuilderWidget(exe_mode=exe_mode)
        self.stacked_widget = QStackedWidget()
        self.stacked_widget.addWidget(self.run_widget)
        self.stacked_widget.addWidget(self.builder_widget)

        # Add the StackedWidget as the central widget of the QMainWindow
        self.setCentralWidget(self.stacked_widget)

        # Create a switch button in the menu bar at the top
        self.switch_button = QAction("Switch to Builder GUI", self)
        self.switch_button.triggered.connect(self.switch_gui)
        self.menuBar().addAction(self.switch_button)

    def switch_gui(self):
        if self.stacked_widget.currentIndex() == 0:
            self.stacked_widget.setCurrentIndex(1)
            self.switch_button.setText("Switch to Run GUI")
        else:
            self.stacked_widget.setCurrentIndex(0)
            self.switch_button.setText("Switch to Builder GUI")


# Replace sys.stdout and sys.stderr only if we're running as a standalone executable
exe_mode=False
if getattr(sys, 'frozen', False):
    exe_mode=True
    class FakeStdOut:
        def write(self, *args, **kwargs):
            pass

        def flush(self, *args, **kwargs):
            pass

    sys.stdout = FakeStdOut()
    sys.stderr = FakeStdOut()

app = QApplication([])
window = Application(exe_mode=exe_mode)
window.show()

# Start the event loop
app.exec_()

# At the end, restore the standard output
sys.stdout = sys.__stdout__

# Continue with the rest of the code after the event loop exits
sys.exit()
