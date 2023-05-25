import threading

from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton, QVBoxLayout, QWidget, QFileDialog, QListWidget, \
    QSplitter, QTextEdit, QLabel, QLineEdit, QHBoxLayout, QStackedWidget, QAction, QComboBox, QScrollArea, \
    QSpinBox, QFormLayout, QDoubleSpinBox, QCheckBox, QGridLayout

from PyQt5.QtCore import Qt, QObject, pyqtSignal
from PyQt5 import QtGui
from PyQt5.QtGui import QTextCursor
import traceback
import sys
import os
import subprocess
import glob
import io
import sys
import re
import json
from jaccard_index.jaccard import jaccard_index

from collections import deque


def excepthook(type, value, tback):
    traceback.print_exception(type, value, tback)
    sys.__excepthook__(type, value, tback)

sys.excepthook = excepthook


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


class JobWidget_grid_old(QWidget):
    def __init__(self, schema, job_number, file_list, parent):
        super().__init__(parent)
        self.parent = parent
        self.schema = schema
        self.file_list = file_list

        self.setStyleSheet("background: transparent;")  # Set background of JobWidget to be transparent

        self.inner_widget = QWidget(self)
        self.inner_widget.setStyleSheet("""
                    QWidget {
                        border: 1px solid blue;
                        border-radius: 5px;
                    }
                """)

        self.job_layout = QGridLayout()
        self.setLayout(self.job_layout)

        # Expand/Collapse button
        self.toggle_button = QPushButton('Expand', self)
        self.toggle_button.clicked.connect(self.toggle)
        self.toggle_button.setStyleSheet("min-width: 60px; min-height: 20px;")  # Adjust the size as needed
        self.job_layout.addWidget(self.toggle_button, 0, 1)  # Add to top middle

        # Delete button
        self.delete_button = QPushButton('✖', self)
        self.delete_button.setObjectName("delete_button")
        self.delete_button.setStyleSheet("delete_button {color: #8b0000; background-color: transparent; border: none;}")
        self.delete_button.clicked.connect(self.delete)
        self.job_layout.addWidget(self.delete_button, 0, 2)  # Add to top right

        # Job Number and Job Type
        self.number_label = QLabel(f'Job {job_number}: ', self)
        self.type_combo_box = QComboBox(self)
        self.type_combo_box.addItems([job['type'] for job in schema])
        self.type_combo_box.currentTextChanged.connect(self.change_job_type)
        self.job_layout.addWidget(self.number_label, 1, 0)  # Add to left middle
        self.job_layout.addWidget(self.type_combo_box, 1, 1, 1, 2)  # Add to middle and make it span two columns

        # Job Name
        self.name_label = QLabel('Job Name: ', self)
        self.name_line_edit = QLineEdit(self)
        self.name_line_edit.setText(self.schema[0]['default_name'])
        self.job_layout.addWidget(self.name_label, 2, 0)  # Add to next row in left column
        self.job_layout.addWidget(self.name_line_edit, 2, 1, 1, 2)  # Add to next row in middle and make it span two columns

        # Parameters
        self.params_layout = QVBoxLayout()
        self.params_layout.setContentsMargins(20, 0, 0, 0)  # Indent the parameters
        self.params_layout.setSpacing(10)  # Adding some spacing between parameters

        # Load parameters
        self.change_job_type()

        # Add parameters layout to job layout in the 3rd row, make it span three columns
        self.job_layout.addLayout(self.params_layout, 3, 0, 1, 3)

    def toggle(self):
        for i in range(self.params_layout.count()):
            widget = self.params_layout.itemAt(i).widget()
            if widget:
                widget.setVisible(not widget.isVisible())
        self.toggle_button.setText('Collapse' if self.toggle_button.text() == 'Expand' else 'Expand')


    def change_job_type(self):
        while self.params_layout.count() > 0:
            item = self.params_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        for job in self.schema:
            if job['type'] == self.type_combo_box.currentText():
                for param, config in job['params'].items():
                    if config['type'] == 'bool':
                        checkbox = QCheckBox(f'{param}: ', self)
                        self.params_layout.addWidget(checkbox)
                    else:
                        label = QLabel(f'{param}: ', self)
                        line_edit = QLineEdit(self)
                        self.params_layout.addWidget(label)
                        self.params_layout.addWidget(line_edit)
                        if 'file' in config:
                            dropdown = QComboBox(self)
                            dropdown.addItems(self.file_list)
                            self.params_layout.addWidget(dropdown)
                            button = QPushButton("Select File", self)
                            self.params_layout.addWidget(button)
                break

    def delete(self):
        self.parent.jobs.remove(self)  # Remove this job from the jobs list
        self.parent.update_job_numbers()  # Update job numbers
        self.deleteLater()

    def job_type_changed(self, job_type):
        # Get job schema
        job_schema = next((job for job in self.schema if job['type'] == job_type), None)
        if not job_schema:
            return

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

    def update_job_numbers(self):
        for i, job in enumerate(self.jobs, start=1):
            job.number_label.setText(f'Job {i}: ')


class JobWidget_old(QWidget):
    def __init__(self, schema, job_number, file_list, parent):
        super().__init__(parent)
        self.parent = parent
        self.schema = schema
        self.file_list = file_list

        self.setStyleSheet("""
            QWidget {
                border: 1px solid blue;
                border-radius: 5px;
            }
            QPushButton#toggle_button {
                background-color: #D3D3D3;
            }
            QPushButton#toggle_button:hover {
                background-color: #A9A9A9;
            }
            QPushButton#delete_button {
                color: #FF0000;
                font-size: 16px;
                background-color: transparent;
                border: none;
            }
        """)  # Set background and border styles of JobWidget

        self.job_layout = QGridLayout()
        self.setLayout(self.job_layout)
        self.job_layout.setColumnStretch(1, 1)
        self.job_layout.setColumnStretch(2, 1)

        # Expand/Collapse button
        self.toggle_button = QPushButton('Expand', self)
        self.toggle_button.setObjectName("toggle_button")
        self.toggle_button.clicked.connect(self.toggle)
        self.job_layout.addWidget(self.toggle_button, 0, 1)  # Add to top middle

        # Delete button
        self.delete_button = QPushButton('✖', self)
        self.delete_button.setObjectName("delete_button")
        self.delete_button.clicked.connect(self.delete)
        self.job_layout.addWidget(self.delete_button, 0, 2)  # Add to top right

        # Job Number and Job Type
        self.number_label = QLabel(f'Job {job_number}: ', self)
        self.type_combo_box = QComboBox(self)
        self.type_combo_box.addItems([job['type'] for job in schema])
        self.type_combo_box.currentTextChanged.connect(self.change_job_type)
        self.job_layout.addWidget(self.number_label, 1, 0)  # Add to left middle
        self.job_layout.addWidget(self.type_combo_box, 1, 1, 1, 2)  # Add to middle and make it span two columns

        # Job Name
        self.name_label = QLabel('Job Name: ', self)
        self.name_line_edit = QLineEdit(self)
        self.name_line_edit.setText(self.schema[0]['default_name'])
        self.job_layout.addWidget(self.name_label, 2, 0)  # Add to next row in left column
        self.job_layout.addWidget(self.name_line_edit, 2, 1, 1,
                                  2)  # Add to next row in middle and make it span two columns

        # Parameters
        self.params_layout = QGridLayout()
        self.params_layout.setContentsMargins(20, 0, 0, 0)  # Indent the parameters
        self.params_layout.setSpacing(10)  # Adding some spacing between parameters

        # Load parameters
        self.change_job_type()

        # Add parameters layout to job layout in the 3rd row, make it span three columns
        self.job_layout.addLayout(self.params_layout, 3, 0, 1, 3)


    def toggle(self):
        for i in range(self.params_layout.count()):
            widget = self.params_layout.itemAt(i).widget()
            if widget:
                widget.setVisible(not widget.isVisible())
        self.toggle_button.setText('Collapse' if self.toggle_button.text() == 'Expand' else 'Expand')

    def change_job_type(self):
        while self.params_layout.count() > 0:
            item = self.params_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        for job in self.schema:
            if job['type'] == self.type_combo_box.currentText():
                for param, config in job['params'].items():
                    if config['type'] == 'bool':
                        checkbox = QCheckBox(f'{param}: ', self)
                        self.params_layout.addWidget(checkbox)
                    else:
                        label = QLabel(f'{param}: ', self)
                        line_edit = QLineEdit(self)
                        self.params_layout.addWidget(label)
                        self.params_layout.addWidget(line_edit)
                        if 'file' in config:
                            dropdown = QComboBox(self)
                            dropdown.addItems(self.file_list)
                            self.params_layout.addWidget(dropdown)
                            button = QPushButton("Select File", self)
                            self.params_layout.addWidget(button)
                break

    def delete(self):
        self.parent.jobs.remove(self)  # Remove this job from the jobs list
        self.parent.update_job_numbers()  # Update job numbers
        self.deleteLater()

    def job_type_changed(self, job_type):
        # Get job schema
        job_schema = next((job for job in self.schema if job['type'] == job_type), None)
        if not job_schema:
            return

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

    def update_job_numbers(self):
        for i, job in enumerate(self.jobs, start=1):
            job.number_label.setText(f'Job {i}: ')


class JobWidget_old2(QWidget):
    def __init__(self, schema, job_number, file_list, parent):
        super().__init__(parent)
        self.parent = parent
        self.schema = schema
        self.file_list = file_list

        self.setStyleSheet("""
                    QWidget {
                        border: 1px solid blue;
                        border-radius: 5px;
                        background: transparent;
                    }
                """)  # Set border and background of JobWidget

        self.job_layout = QGridLayout()
        self.setLayout(self.job_layout)

        # Job Number and Job Type
        self.number_label = QLabel(f'{job_number}', self)
        self.number_label.setStyleSheet("""
            QLabel {
                padding-left: 10px;   /* Left padding to create some space between the border and the number */
            }
        """)
        self.type_combo_box = QComboBox(self)
        self.type_combo_box.addItems([job['type'] for job in schema])
        self.type_combo_box.currentTextChanged.connect(self.change_job_type)
        self.job_layout.addWidget(self.number_label, 0, 0, 3, 1)  # Add to the first three rows in left column
        self.job_layout.addWidget(self.type_combo_box, 0, 1, 1, 2)  # Add to top row, make it span two columns

        # Expand/Collapse button
        self.toggle_button = QPushButton('Expand', self)
        self.toggle_button.clicked.connect(self.toggle)
        self.toggle_button.setStyleSheet("""
            QPushButton {
                min-width: 60px; 
                min-height: 20px; 
                background-color: #d3d3d3;
            }
        """)  # Set size and make it look like a grey button
        self.job_layout.addWidget(self.toggle_button, 0, 2)  # Add to top row in the right column

        # Delete button
        self.delete_button = QPushButton('✖', self)
        self.delete_button.setStyleSheet("""
            QPushButton {
                color: #8b0000; 
                background-color: transparent; 
                border: none;
                font-size: 20px;  /* Make it bigger */
            }
        """)  # Set style to be a red and obvious X
        self.delete_button.clicked.connect(self.delete)
        self.job_layout.addWidget(self.delete_button, 0, 3)  # Add to top row in the far right column

        # Job Name
        self.name_label = QLabel('Job Name: ', self)
        self.name_line_edit = QLineEdit(self)
        self.name_line_edit.setText(self.schema[0]['default_name'])
        self.job_layout.addWidget(self.name_label, 1, 1)  # Add to second row in the second column
        self.job_layout.addWidget(self.name_line_edit, 1, 2, 1,
                                  2)  # Add to second row in the third and fourth columns

        # Parameters scroll area
        self.params_scroll_area = QScrollArea()
        self.params_scroll_area.setWidgetResizable(True)
        self.params_widget = QWidget()
        self.params_scroll_area.setWidget(self.params_widget)
        self.params_layout = QGridLayout()
        self.params_widget.setLayout(self.params_layout)
        self.params_scroll_area.setStyleSheet("""
                    QScrollArea {
                        border: none;
                        background: transparent;
                    }
                """)
        self.job_layout.addWidget(self.params_scroll_area, 2, 1, 1, 3)  # Add to third row, make it span three columns

        # Load parameters
        self.change_job_type()

    def toggle(self):
        for i in range(self.params_layout.count()):
            widget = self.params_layout.itemAt(i).widget()
            if widget:
                widget.setVisible(not widget.isVisible())
        self.toggle_button.setText('Collapse' if self.toggle_button.text() == 'Expand' else 'Expand')

    def change_job_type(self):
        while self.params_layout.count() > 0:
            item = self.params_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        for job in self.schema:
            if job['type'] == self.type_combo_box.currentText():
                for param, config in job['params'].items():
                    if config['type'] == 'bool':
                        checkbox = QCheckBox(f'{param}: ', self)
                        self.params_layout.addWidget(checkbox)
                    else:
                        label = QLabel(f'{param}: ', self)
                        line_edit = QLineEdit(self)
                        self.params_layout.addWidget(label)
                        self.params_layout.addWidget(line_edit)
                        if 'file' in config:
                            dropdown = QComboBox(self)
                            dropdown.addItems(self.file_list)
                            self.params_layout.addWidget(dropdown)
                            button = QPushButton("Select File", self)
                            self.params_layout.addWidget(button)
                break

    def delete(self):
        self.parent.jobs.remove(self)  # Remove this job from the jobs list
        self.parent.update_job_numbers()  # Update job numbers
        self.deleteLater()

    def job_type_changed(self, job_type):
        # Get job schema
        job_schema = next((job for job in self.schema if job['type'] == job_type), None)
        if not job_schema:
            return

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

    def update_job_numbers(self):
        for i, job in enumerate(self.jobs, start=1):
            job.number_label.setText(f'Job {i}: ')


class JobWidget_old3(QWidget):
    def __init__(self, schema, job_number, file_list, parent):
        super().__init__(parent)
        self.parent = parent
        self.schema = schema
        self.file_list = file_list

        self.setStyleSheet("""
            QWidget {
                border: 1px solid blue;
                border-radius: 5px;
                padding: 10px;
            }
        """)

        self.job_layout = QGridLayout()
        self.setLayout(self.job_layout)
        self.job_layout.setSpacing(10)

        # Job Number
        self.number_label = QLabel(f'Job {job_number}: ', self)
        self.job_layout.addWidget(self.number_label, 1, 0)

        # Expand/Collapse button
        self.toggle_button = QPushButton('Expand', self)
        self.toggle_button.clicked.connect(self.toggle)
        self.toggle_button.setStyleSheet("""
            QPushButton {
                min-width: 60px; 
                min-height: 20px;
                background: #D3D3D3;
            }
        """)
        self.job_layout.addWidget(self.toggle_button, 0, 1)

        # Delete button
        self.delete_button = QPushButton('✖', self)
        self.delete_button.setStyleSheet("""
            QPushButton {
                color: #8b0000; 
                background-color: transparent; 
                border: none;
                font-size: 20px;
            }
        """)
        self.delete_button.clicked.connect(self.delete)
        self.job_layout.addWidget(self.delete_button, 0, 2)

        # Job Type
        self.type_label = QLabel('Job Type: ', self)
        self.type_combo_box = QComboBox(self)
        self.type_combo_box.addItems([job['type'] for job in schema])
        self.type_combo_box.currentTextChanged.connect(self.change_job_type)
        self.job_layout.addWidget(self.type_label, 1, 1)
        self.job_layout.addWidget(self.type_combo_box, 1, 2)

        # Parameters
        self.params_widget = QWidget()
        self.params_layout = QGridLayout()
        self.params_widget.setLayout(self.params_layout)
        self.params_layout.setHorizontalSpacing(10)
        self.job_layout.addWidget(self.params_widget, 2, 1, 1, 2)

        # Load parameters
        self.change_job_type()

    def toggle(self):
        for i in range(self.params_layout.count()):
            widget = self.params_layout.itemAt(i).widget()
            if widget:
                widget.setVisible(not widget.isVisible())
        self.toggle_button.setText('Expand' if self.toggle_button.text() == 'Collapse' else 'Collapse')

    def change_job_type(self):
        while self.params_layout.count() > 0:
            item = self.params_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        for job in self.schema:
            if job['type'] == self.type_combo_box.currentText():
                row_index = 0
                for param, config in job['params'].items():
                    if config['type'] == 'bool':
                        checkbox = QCheckBox(f'{param}: ', self)
                        self.params_layout.addWidget(checkbox, row_index, 0)
                    else:
                        label = QLabel(f'{param}: ', self)
                        line_edit = QLineEdit(self)
                        self.params_layout.addWidget(label, row_index, 0)
                        self.params_layout.addWidget(line_edit, row_index, 1)
                        if 'file' in config:
                            dropdown = QComboBox(self)
                            dropdown.addItems(self.file_list)
                            self.params_layout.addWidget(dropdown, row_index, 2)
                            button = QPushButton("Select File", self)
                            self.params_layout.addWidget(button, row_index, 3)
                    row_index += 1
                break

    def delete(self):
        self.parent.jobs.remove(self)  # Remove this job from the jobs list
        self.parent.update_job_numbers()  # Update job numbers
        self.deleteLater()

    def job_type_changed(self, job_type):
        # Get job schema
        job_schema = next((job for job in self.schema if job['type'] == job_type), None)
        if not job_schema:
            return

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

    def update_job_numbers(self):
        for i, job in enumerate(self.jobs, start=1):
            job.number_label.setText(f'Job {i}: ')


class JobWidget_old4(QWidget):
    def __init__(self, schema, job_number, file_list, parent):
        super().__init__(parent)
        self.parent = parent
        self.schema = schema
        self.file_list = file_list

        self.job_layout = QGridLayout()
        self.setLayout(self.job_layout)
        self.job_layout.setSpacing(10)

        # Job Number
        self.number_label = QLabel(f'Job {job_number}', self)
        self.number_label.setAlignment(Qt.AlignCenter)
        self.number_label.setStyleSheet("font-size: 18px;")
        self.job_layout.addWidget(self.number_label, 0, 0, 3, 1)  # Spanning 3 rows

        # Expand/Collapse button
        self.toggle_button = QPushButton('Expand', self)
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

        # Delete button
        self.delete_button = QPushButton('✖', self)
        self.delete_button.setStyleSheet("""
                    QPushButton {
                        color: #8b0000; 
                        background-color: transparent; 
                        border: none;
                        font-size: 20px;
                    }
                """)
        self.delete_button.clicked.connect(self.delete)
        self.job_layout.addWidget(self.delete_button, 0, 2)

        # Job Type
        self.type_label = QLabel('Job Type: ', self)
        self.type_combo_box = QComboBox(self)
        self.type_combo_box.addItems([job['type'] for job in schema])
        self.type_combo_box.setStyleSheet("color: white;")
        self.type_combo_box.currentTextChanged.connect(self.change_job_type)
        self.job_layout.addWidget(self.type_label, 1, 1)
        self.job_layout.addWidget(self.type_combo_box, 1, 2)

        # Job Name
        self.name_label = QLabel('Job Name: ', self)
        self.name_line_edit = QLineEdit(self)
        self.name_line_edit.setText(self.schema[0]['default_name'])
        self.job_layout.addWidget(self.name_label, 2, 1)
        self.job_layout.addWidget(self.name_line_edit, 2, 2)

        # Parameters
        self.params_widget = QWidget()
        self.params_layout = QGridLayout()
        self.params_widget.setLayout(self.params_layout)
        self.params_layout.setHorizontalSpacing(10)
        self.job_layout.addWidget(self.params_widget, 3, 1, 1, 2)

        # Load parameters
        self.change_job_type()

    def toggle(self):
        for i in range(self.params_layout.count()):
            widget = self.params_layout.itemAt(i).widget()
            if widget:
                widget.setVisible(not widget.isVisible())
        self.toggle_button.setText('Expand' if self.toggle_button.text() == 'Collapse' else 'Collapse')

    def change_job_type(self):
        while self.params_layout.count() > 0:
            item = self.params_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        for job in self.schema:
            if job['type'] == self.type_combo_box.currentText():
                row_index = 0
                for param, config in job['params'].items():
                    if config['type'] == 'bool':
                        checkbox = QCheckBox(f'{param}: ', self)
                        self.params_layout.addWidget(checkbox, row_index, 0)
                    else:
                        label = QLabel(f'{param}: ', self)
                        line_edit = QLineEdit(self)
                        self.params_layout.addWidget(label, row_index, 0)
                        self.params_layout.addWidget(line_edit, row_index, 1)
                        if 'file' in config:
                            dropdown = QComboBox(self)
                            dropdown.addItems(self.file_list)
                            self.params_layout.addWidget(dropdown, row_index, 2)
                            button = QPushButton("Select File", self)
                            self.params_layout.addWidget(button, row_index, 3)
                    row_index += 1
                break

    def delete(self):
        self.parent.jobs.remove(self)  # Remove this job from the jobs list
        self.parent.update_job_numbers()  # Update job numbers
        self.deleteLater()

    def job_type_changed(self, job_type):
        # Get job schema
        job_schema = next((job for job in self.schema if job['type'] == job_type), None)
        if not job_schema:
            return

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

    def update_job_numbers(self):
        for i, job in enumerate(self.jobs, start=1):
            job.number_label.setText(f'Job {i}: ')


class JobWidget(QWidget):
    def __init__(self, schema, job_number, file_list, parent):
        super().__init__(parent)
        self.setObjectName("jobWidget")  # Setting object name for stylesheet targeting
        self.parent = parent
        self.schema = schema
        self.file_list = file_list

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
                """)

        self.job_layout = QGridLayout()
        self.setLayout(self.job_layout)
        self.job_layout.setSpacing(10)

        # Job Number
        self.number_label = QLabel(f'Job {job_number}', self)
        self.number_label.setAlignment(Qt.AlignCenter)
        self.number_label.setStyleSheet("font-size: 18px;")
        self.job_layout.addWidget(self.number_label, 0, 0, 3, 1)  # Spanning 3 rows

        # Expand/Collapse button
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

        # Delete button
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

        # Job Type
        self.type_label = QLabel('Job Type: ', self)
        self.type_combo_box = QComboBox(self)
        self.type_combo_box.addItems([job['type'] for job in schema])
        self.type_combo_box.setStyleSheet("color: white;")
        self.type_combo_box.currentTextChanged.connect(self.change_job_type)
        self.job_layout.addWidget(self.type_label, 1, 1)
        self.job_layout.addWidget(self.type_combo_box, 1, 2)

        # Job Name
        self.name_label = QLabel('Job Name: ', self)
        self.name_line_edit = QLineEdit(self)
        self.name_line_edit.setText(self.schema[0]['default_name'])
        self.job_layout.addWidget(self.name_label, 2, 1)
        self.job_layout.addWidget(self.name_line_edit, 2, 2)

        # Parameters
        self.params_widget = QWidget()
        self.params_layout = QGridLayout()
        self.params_widget.setLayout(self.params_layout)
        self.params_layout.setHorizontalSpacing(10)
        self.job_layout.addWidget(self.params_widget, 3, 1, 1, 2)

        # Load parameters
        self.change_job_type()

    def toggle(self):
        for i in range(self.params_layout.count()):
            widget = self.params_layout.itemAt(i).widget()
            if widget:
                widget.setVisible(not widget.isVisible())
        self.toggle_button.setText('Expand' if self.toggle_button.text() == 'Collapse' else 'Collapse')

    def change_job_type(self):
        while self.params_layout.count() > 0:
            item = self.params_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        for job in self.schema:
            if job['type'] == self.type_combo_box.currentText():
                row_index = 0
                for param, config in job['params'].items():
                    if config['type'] == 'bool':
                        checkbox = QCheckBox(f'{param}: ', self)
                        self.params_layout.addWidget(checkbox, row_index, 0)
                    else:
                        label = QLabel(f'{param}: ', self)
                        line_edit = QLineEdit(self)
                        self.params_layout.addWidget(label, row_index, 0)
                        self.params_layout.addWidget(line_edit, row_index, 1)
                        if 'file' in config:
                            dropdown = QComboBox(self)
                            dropdown.addItems(self.file_list)
                            self.params_layout.addWidget(dropdown, row_index, 2)
                            button = QPushButton("Select File", self)
                            self.params_layout.addWidget(button, row_index, 3)
                    row_index += 1
                break

    def delete(self):
        self.parent.jobs.remove(self)  # Remove this job from the jobs list
        self.parent.update_job_numbers()  # Update job numbers
        self.deleteLater()

    def job_type_changed(self, job_type):
        # Get job schema
        job_schema = next((job for job in self.schema if job['type'] == job_type), None)
        if not job_schema:
            return

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

    def update_job_numbers(self):
        for i, job in enumerate(self.jobs, start=1):
            job.number_label.setText(f'Job {i}: ')


class BuilderWidget(QWidget):
    def __init__(self):
        super().__init__()

        self.main_layout = QVBoxLayout()
        self.setLayout(self.main_layout)

        # File selector
        self.file_label = QLabel("JSON Config File:", self)
        self.file_line_edit = QLineEdit(self)
        self.file_label.setBuddy(self.file_line_edit)  # Associate the label with the QLineEdit

        self.file_button = QPushButton("Select JSON Config", self)
        self.file_button.clicked.connect(self.file_dialog)
        self.file_dropdown = QComboBox(self)
        self.file_dropdown.addItems([f for f in os.listdir() if re.match(r'^config.*\.json$', f)])

        # Connect change in dropdown to the QLineEdit
        self.file_dropdown.currentTextChanged.connect(self.file_line_edit.setText)

        # File selector layout
        file_layout = QVBoxLayout()
        file_line_layout = QHBoxLayout()
        file_dropdown_layout = QHBoxLayout()
        file_line_layout.addWidget(self.file_label)
        file_line_layout.addWidget(self.file_line_edit)
        file_dropdown_layout.addWidget(self.file_button)
        file_dropdown_layout.addWidget(self.file_dropdown)
        file_layout.addLayout(file_line_layout)
        file_layout.addLayout(file_dropdown_layout)
        self.main_layout.addLayout(file_layout)

        # Load schema
        with open('job_schema.json') as f:
            self.schema = json.load(f)['job_schema']
        self.jobs = []

        # Jobs layout
        self.jobs_layout = QVBoxLayout()
        self.jobs_layout.setSpacing(0)  # Adding some spacing between jobs for better readability

        # Add job button
        self.add_job_button = QPushButton("+ Add Job +", self)
        self.add_job_button.clicked.connect(self.add_job)
        self.main_layout.addWidget(self.add_job_button)

        # Jobs scroll area
        self.jobs_scroll_area = QScrollArea()
        self.jobs_scroll_area.setWidgetResizable(True)
        self.jobs_widget = QWidget()
        self.jobs_scroll_area.setWidget(self.jobs_widget)
        self.jobs_widget.setStyleSheet("background: transparent;")
        self.jobs_widget.setLayout(self.jobs_layout)
        self.jobs_scroll_area.setStyleSheet(
            "QScrollArea {border: 1px solid white; border-radius: 5px; background: transparent;}")
        self.main_layout.addWidget(self.jobs_scroll_area)

        # Initialize the file list
        self.file_list = []

        # Save button
        self.save_button = QPushButton("Save Config", self)
        self.save_button.setStyleSheet("color: #FFFFFF; background-color: #008000;")
        self.save_button.clicked.connect(self.save_config)
        self.main_layout.addWidget(self.save_button)

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

    def add_job(self):
        job_widget = JobWidget(self.schema, len(self.jobs) + 1, self.file_list, self)
        self.jobs_layout.addWidget(job_widget)
        self.jobs.append(job_widget)
        self.update_job_numbers()
        self.scroll_to_bottom()

    def save_config(self):
        # This function needs to be implemented to save the current configuration to the file specified in file_line_edit
        pass


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


class Application(QMainWindow):
    def __init__(self):
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

        # Create an instance of EmittingStream and hold onto it
        self.emitting_stream = EmittingStream(self)

        # Create the Run and Builder widgets and add them to the stacked widget
        self.run_widget = RunWidget(keywords=['Filter', 'rows', 'scores', 'Calculating'],
                                    past_lines=5)
        self.builder_widget = BuilderWidget()
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


app = QApplication([])
window = Application()
window.show()
sys.exit(app.exec_())
