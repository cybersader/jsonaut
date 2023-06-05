from PyQt5.QtCore import QRegExp, Qt, QSize, QRect
from PyQt5.QtGui import QColor, QSyntaxHighlighter, QTextCharFormat, QFont, QTextCursor, QIcon, QPainter
from PyQt5.QtWidgets import QApplication, QDialog, QVBoxLayout, QPlainTextEdit, QPushButton, QMessageBox, QShortcut, \
    QLabel, QHBoxLayout, QToolTip, QSplitter, QWidget
from PyQt5.QtGui import QKeySequence
from jsonschema import validate, ValidationError
import json
import sys


class LineNumberArea(QWidget):
    def __init__(self, editor):
        super().__init__(editor)
        self.codeEditor = editor

    def sizeHint(self):
        return QSize(self.codeEditor.lineNumberAreaWidth(), 0)

    def paintEvent(self, event):
        self.codeEditor.lineNumberAreaPaintEvent(event)


class CodeEditor(QPlainTextEdit):
    def __init__(self, *args):
        super(CodeEditor, self).__init__(*args)
        self.lineNumberArea = LineNumberArea(self)
        self.blockCountChanged.connect(self.updateLineNumberAreaWidth)
        self.updateRequest.connect(self.updateLineNumberArea)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)

    def lineNumberAreaWidth(self):
        digits = 1
        max_ = max(1, self.blockCount())
        while max_ >= 10:
            max_ /= 10
            digits += 1
        space = 3 + self.fontMetrics().horizontalAdvance('9') * digits
        return space

    def updateLineNumberAreaWidth(self, _):
        self.setViewportMargins(self.lineNumberAreaWidth(), 0, 0, 0)

    def updateLineNumberArea(self, rect, dy):
        if dy:
            self.lineNumberArea.scroll(0, dy)
        else:
            self.lineNumberArea.update(0, rect.y(), self.lineNumberArea.width(), rect.height())

    def resizeEvent(self, event):
        super().resizeEvent(event)
        cr = self.contentsRect()
        self.lineNumberArea.setGeometry(QRect(cr.left(), cr.top(), self.lineNumberAreaWidth(), cr.height()))

    def lineNumberAreaPaintEvent(self, event):
        painter = QPainter(self.lineNumberArea)
        painter.fillRect(event.rect(), Qt.darkGray)
        block = self.firstVisibleBlock()
        blockNumber = block.blockNumber()
        top = int(self.blockBoundingGeometry(block).translated(self.contentOffset()).top())
        bottom = top + int(self.blockBoundingRect(block).height())
        while block.isValid() and top <= event.rect().bottom():
            if block.isVisible() and bottom >= event.rect().top():
                number = str(blockNumber + 1)
                painter.setPen(Qt.white)
                painter.drawText(0, top, self.lineNumberArea.width(), self.fontMetrics().height(),
                                 Qt.AlignRight, number)
            block = block.next()
            top = bottom
            bottom = top + int(self.blockBoundingRect(block).height())
            blockNumber += 1

    def wheelEvent(self, e):
        if e.modifiers() == Qt.ControlModifier:
            if e.angleDelta().y() > 0:
                self.zoomIn()
            else:
                self.zoomOut()
        else:
            super().wheelEvent(e)

    def keyPressEvent(self, e):
        if e.key() == Qt.Key_Tab:
            cursor = self.textCursor()
            cursor.insertText('  ')
        else:
            super().keyPressEvent(e)


class JsonHighlighter(QSyntaxHighlighter):
    def __init__(self, document):
        QSyntaxHighlighter.__init__(self, document)

        self.json_keyword_format = QTextCharFormat()
        self.json_keyword_format.setForeground(QColor("lightblue"))
        self.json_keyword_format.setFontWeight(QFont.Bold)

        self.json_string_format = QTextCharFormat()
        self.json_string_format.setForeground(QColor("yellow"))

        self.json_special_format = QTextCharFormat()
        self.json_special_format.setForeground(QColor("magenta"))

    def highlightBlock(self, text):
        expression = QRegExp("\".*\" *:")
        index = expression.indexIn(text)
        while index >= 0:
            length = expression.matchedLength()
            self.setFormat(index, length, self.json_keyword_format)
            index = expression.indexIn(text, index + length)

        expression = QRegExp(": *\".*\"")
        index = expression.indexIn(text)
        while index >= 0:
            length = expression.matchedLength()
            self.setFormat(index, length, self.json_string_format)
            index = expression.indexIn(text, index + length)

        expression = QRegExp("null|false|true")
        index = expression.indexIn(text)
        while index >= 0:
            length = expression.matchedLength()
            self.setFormat(index, length, self.json_special_format)
            index = expression.indexIn(text, index + length)


class DictEditor(QDialog):
    def __init__(self, schema, data, options, close_on_save=True):
        super().__init__()
        self.schema = schema
        self.data = data
        self.options = options
        self.close_on_save = close_on_save

        self.initUI()

    def initUI(self):
        self.setWindowTitle("JSON Editor")
        self.setGeometry(300, 300, 600, 400)

        layout = QVBoxLayout()
        self.setLayout(layout)

        splitter = QSplitter(Qt.Horizontal)

        self.text_editor = CodeEditor()
        self.text_editor.setStyleSheet("""
                    CodeEditor {
                        background-color: #2b2b2b;
                        color: #a9b7c6;
                        font-family: "Courier New";
                    }
                """)
        self.text_editor.setPlainText(json.dumps(self.data, indent=2))
        self.highlighter = JsonHighlighter(self.text_editor.document())

        self.options_editor = CodeEditor()
        self.options_editor.setReadOnly(True)
        self.options_editor.setVisible(False)

        splitter.addWidget(self.text_editor)
        splitter.addWidget(self.options_editor)

        layout.addWidget(splitter)

        buttons_layout = QHBoxLayout()

        save_button = QPushButton(QIcon("save.png"), "Save")
        save_button.clicked.connect(self.save_json)
        buttons_layout.addWidget(save_button)

        prettify_button = QPushButton(QIcon("prettify.png"), "Prettify")
        prettify_button.clicked.connect(self.prettify_json)
        QToolTip.setFont(QFont('SansSerif', 10))
        prettify_button.setToolTip('Shortcut: <b>Ctrl+P</b>')
        buttons_layout.addWidget(prettify_button)

        show_options_button = QPushButton(QIcon("options.png"), "Show Options")
        show_options_button.clicked.connect(self.show_options)
        buttons_layout.addWidget(show_options_button)

        wrap_button = QPushButton(QIcon("wrap.png"), "Toggle Wrap")
        wrap_button.clicked.connect(self.toggle_wrap)
        buttons_layout.addWidget(wrap_button)

        zoom_in_button = QPushButton(QIcon("zoom_in.png"), "Zoom In")
        zoom_in_button.clicked.connect(self.text_editor.zoomIn)
        buttons_layout.addWidget(zoom_in_button)

        zoom_out_button = QPushButton(QIcon("zoom_out.png"), "Zoom Out")
        zoom_out_button.clicked.connect(self.text_editor.zoomOut)
        buttons_layout.addWidget(zoom_out_button)

        layout.addLayout(buttons_layout)

        prettify_shortcut = QShortcut(QKeySequence("Ctrl+P"), self)
        prettify_shortcut.activated.connect(self.prettify_json)

    def save_json(self):
        try:
            editor_content = self.text_editor.toPlainText().strip()
            if editor_content:
                data = json.loads(editor_content)
                if self.schema:  # Validate only if schema exists
                    validate(instance=data, schema=self.schema)
                self.data = data
            else:
                self.data = None
            if self.close_on_save:
                self.close()
                self.parent().close()
        except ValueError as e:
            QMessageBox.critical(self, "Error", "Invalid JSON data. " + str(e))
        except ValidationError as e:
            QMessageBox.critical(self, "Error", "JSON data does not match the schema. " + str(e))

    def prettify_json(self):
        try:
            data = json.loads(self.text_editor.toPlainText())
            pretty_data = json.dumps(data, indent=2)
            self.text_editor.setPlainText(pretty_data)
        except ValueError as e:
            QMessageBox.critical(self, "Error", "Invalid JSON data. " + str(e))

    def show_options(self):
        self.options_editor.setVisible(True)
        self.options_editor.setPlainText('Options: ^^^^\n')
        for key, option in self.options.items():
            self.options_editor.insertPlainText(f'# {key}: {option}\n')

    def get_value(self):
        return self.data

    def toggle_wrap(self):
        if self.text_editor.lineWrapMode() == QPlainTextEdit.NoWrap:
            self.text_editor.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        else:
            self.text_editor.setLineWrapMode(QPlainTextEdit.NoWrap)


def main():
    schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "number"}
        },
        "required": ["name", "age"]
    }

    data = {
        "name": "John Doe",
        "age": 30
    }

    options = {
        "name": "A string representing the name.",
        "age": "A number representing the age."
    }

    app = QApplication(sys.argv)
    editor = DictEditor(schema, data, options)
    editor.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()