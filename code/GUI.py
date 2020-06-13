import os
import sys
import qdarkstyle
from API import str_main, init, sql_exit
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.Qsci import *

comment_grey = QColor("#5c6370")
gutter_grey = QColor("#4b5263")
cyan = QColor("#56b6c2")
magenta = QColor("c678dd")
blue = QColor("#61afef")
dark_yellow = QColor("#d19a66")
light_yellow = QColor("#e5c07b")
green = QColor("#98c379")
dark_red = QColor("#be5046")
light_red = QColor("#e06c75")
white = QColor("#abb2bf")
black = QColor("#282c34")
purple = QColor("#d55fdd")
font_name_ui = "Ubuntu"
font_name_mono = "UbuntuMono Nerd Font"
window_font_scale = 2
base_font_point_size = 14*window_font_scale


class OutputLexer(QsciLexerCustom):
    def __init__(self, parent):
        super(OutputLexer, self).__init__(parent)
        self.setDefaultColor(white)
        self.setDefaultPaper(black)
        self.default_font = QFont(font_name_mono)
        self.default_font.setItalic(False)
        self.default_font.setBold(False)
        self.default_font.setPointSize(base_font_point_size)
        self.setFont(self.default_font)

        self.bold_font = QFont(font_name_mono)
        self.bold_font.setBold(True)
        self.bold_font.setItalic(True)
        self.bold_font.setPointSize(base_font_point_size)

        self.setColor(light_red, 1)
        self.setColor(dark_yellow, 2)
        self.setColor(blue, 3)
        self.setColor(self.color(1), 4)
        self.setFont(self.bold_font, 4)
        self.setColor(self.color(2), 5)
        self.setFont(self.bold_font, 5)
        self.setColor(self.color(3), 6)
        self.setFont(self.bold_font, 6)

    def styleText(self, start, end):
        # 1. Initialize the styling procedure
        # ------------------------------------
        self.startStyling(start)

        # 2. Slice out a part from the text
        # ----------------------------------
        text = self.parent().text()[start:end]

        lines = text.split("\n")  # split by line
        # print(lines)
        # print(repr(text))
        for i, line in enumerate(lines):
            if len(line) >= 4 and line[0:4].lower() == "info":
                print("We've got an info")
                self.setStyling(4, 6)
                self.setStyling(len(line) - 4, 3)
            elif len(line) >= 5 and line[0:5].lower() == "error":
                print("We've got an error")
                self.setStyling(5, 4)
                self.setStyling(len(line) - 5, 1)
            elif len(line) >= 7 and line[0:7].lower() == "warning":
                print("We've got an warning")
                self.setStyling(7, 5)
                self.setStyling(len(line) - 7, 2)
            else:
                self.setStyling(len(line), 0)

            if i != len(lines) - 1:
                self.setStyling(1, 0)

    def description(self, style):
        if style == 0:
            return "Default"
        elif style == 1:
            return "Error"
        elif style == 2:
            return "Warning"
        elif style == 3:
            return "Info"
        elif style == 4:
            return "Error keyword"
        elif style == 5:
            return "Warning Keyword"
        elif style == 6:
            return "Info Keyword"
        return ""


class MiniSQLLexer(QsciLexerSQL):
    def __init__(self, parent):
        super(MiniSQLLexer, self).__init__(parent)

        self.setDefaultColor(white)
        self.setDefaultPaper(black)
        self.default_font = QFont(font_name_mono)
        self.default_font.setItalic(False)
        self.default_font.setBold(False)
        self.default_font.setPointSize(base_font_point_size)
        self.setDefaultFont(self.default_font)
        self.setFont(self.default_font)

        self.setColor(white, 0)
        self.comment_font = QFont(font_name_mono)
        self.comment_font.setPointSize(base_font_point_size * 0.9)
        self.comment_font.setItalic(True)
        self.comment_font.setBold(False)
        self.setColor(comment_grey, 1)
        self.setColor(comment_grey, 2)
        self.setFont(self.comment_font, 1)
        self.setFont(self.comment_font, 2)

        self.setColor(green, 6)
        self.setColor(green, 7)
        self.setColor(dark_yellow, 4)
        self.setColor(light_yellow, 8)
        self.setColor(light_red, 9)
        self.setColor(white, 11)
        self.setColor(blue, 5)
        self.keyword_font = QFont(font_name_mono)
        self.keyword_font.setBold(True)
        self.keyword_font.setItalic(True)
        self.keyword_font.setPointSize(14)
        self.setFont(self.keyword_font, 5)
        self.setColor(purple, 10)
        self.setColor(magenta, 3)
        self.setColor(comment_grey, 15)
        self.setColor(comment_grey, 13)


class CustomMainWindow(QMainWindow):
    def __init__(self):
        super(CustomMainWindow, self).__init__()
        init()
        # Window setup
        # --------------

        # 1. Define the geometry of the main window
        init_geometry = (300, 300, 800*window_font_scale, 400*window_font_scale)

        self.setGeometry(*init_geometry)
        self.setWindowTitle("miniSQL GUI")

        # 2. Create frame and layout
        self.frame = QFrame(self)
        self.layout = QVBoxLayout()
        self.frame.setLayout(self.layout)
        self.setCentralWidget(self.frame)
        self.myFontMono = QFont()
        self.myFontMono.setPointSize(base_font_point_size)
        # self.__myFontMono.setFamily("CMU Typewriter Text BoldItalic")
        self.myFontMono.setFamily(font_name_mono)
        self.myFontUI = QFont()
        self.myFontUI.setPointSize(base_font_point_size)
        self.myFontUI.setFamily(font_name_ui)

        # 3. Place a button
        self.hbox = QHBoxLayout()
        self.hbox.addStretch(1)

        self.button_run = QPushButton("Run")
        self.button_run.setFixedWidth(80)
        self.button_run.setFixedHeight(40)
        self.button_run.clicked.connect(self.run_sql)
        self.button_run.setFont(self.myFontUI)
        self.hbox.addWidget(self.button_run)

        self.button_run = QPushButton("Quit")
        self.button_run.setFixedWidth(80)
        self.button_run.setFixedHeight(40)
        self.button_run.clicked.connect(self.exit_sql)
        self.button_run.setFont(self.myFontUI)
        self.hbox.addWidget(self.button_run)

        self.layout.addLayout(self.hbox)
        # QScintilla editor setup
        # ------------------------

        # ! Make instance of QsciScintilla class!
        self.editor = QsciScintilla()
        # adding the SQL lexer to the editor
        self.sql_lexer = MiniSQLLexer(self.editor)
        self.api = QsciAPIs(self.sql_lexer)
        # The moment you create the object, you plug it into the lexer immediately.
        # That's why you pass it your lexer as a parameter.
        auto_completions = [
            "select",
            "show",
            "index",
            "delete",
            "drop",
            "from",
            "where",
            "tables",
            "table",
            "create",
        ]
        for ac in auto_completions:
            self.api.add(ac)
        self.api.prepare()

        self.editor.setLexer(self.sql_lexer)

        self.editor.setAutoCompletionSource(QsciScintilla.AcsAll)
        self.editor.setAutoCompletionThreshold(1)
        self.setupEditorStyle(self.editor)
        # ! Add editor to layout !
        self.layout.addWidget(self.editor)

        self.output = QsciScintilla()
        self.out_lexer = OutputLexer(self.output)
        self.output.setLexer(self.out_lexer)


        self.output.setReadOnly(True)
        self.output.setText(
            "Welcome to miniSQL GUI!\nThis is the output window\nThe editor above is where you input SQL queries, you'll find some interesting keyboard shortcuts there\n")
        """
        some interesting keyboard shortcuts would include fancy stuff
        like multiline editing: Alt + Shift + Arrow
        and line deletion: Ctrl + L
        """
        self.output.append("Error: table table_name is not found\n")
        self.output.append("Warning: key is duplicated and we're replacing it\n")
        self.output.append("Info: we've added 100 rows in 0.01s")
        self.setupEditorStyle(self.output)
        self.layout.addWidget(self.output)

        # self.commands = self.editor.standardCommands()
        # command = self.commands.boundTo(Qt.ControlModifier | Qt.Key_R)
        # if command is not None:
        #     print(command)
        #     command.setKey(0)  # clear the default
        self.run_sql_key_comb = QShortcut(Qt.ControlModifier | Qt.Key_R, self)
        self.run_sql_key_comb.activated.connect(self.run_sql)
        self.exit_sql_key_comb = QShortcut(Qt.ControlModifier | Qt.Key_Q, self)
        self.exit_sql_key_comb.activated.connect(self.exit_sql)
        self.editor.setFocus()
        self.show()

    def setupEditorStyle(self, editor):
        editor.setUtf8(True)  # Set encoding to UTF-8
        editor.setFont(self.myFontUI)  # Will be overridden by lexer!
        editor.setMarginsBackgroundColor(black.darker(120))
        editor.setMarginsForegroundColor(white.darker(120))
        editor.setMarginType(0, QsciScintilla.NumberMargin)
        editor.setMarginWidth(0, "000")
        editor.setMarginsFont(self.myFontMono)
        editor.setScrollWidth(1)  # I'd like to set this to 0, however it doesn't work
        editor.setScrollWidthTracking(True)
        editor.setCaretForegroundColor(white)
        editor.setCaretWidth(3)
        editor.setSelectionBackgroundColor(black.lighter())
        editor.resetSelectionForegroundColor()  # don't change foreground color of selection
        editor.setWrapMode(QsciScintilla.WrapWord)
        editor.setWrapVisualFlags(QsciScintilla.WrapFlagByText)
        editor.setWrapIndentMode(QsciScintilla.WrapIndentSame)

    ''''''

    def run_sql(self):
        print("You've pressed the run button/done the key combination, will it run?")
        content = self.editor.text()
        # todo: actually run the command
        print(content)
        self.output.setText(str_main(content))

    def exit_sql(self):
        print("You've decided to leave right?")
        # todo: call miniSQL stuff to finish up
        sql_exit()
        self.close()

    ''''''


''' End Class '''

def main():
    app = QApplication(sys.argv)
    font_db = QFontDatabase()
    font_name_list = os.listdir("font")

    for font_name in font_name_list:
        if font_name.endswith("ttf") or font_name.endswith("otf") or font_name.endswith("ttc"):
            font_full_name = "font/"+font_name
            font_stream = QFile(font_full_name)
            if font_stream.open(QFile.ReadOnly):
                font_data = font_stream.readAll()
                font_id = font_db.addApplicationFontFromData(font_data)
                families = font_db.applicationFontFamilies(font_id)
    # todo: change to svg icon
    app.setWindowIcon(QIcon('figure/miniSQL.svg'))
    style_sheet = qdarkstyle.load_stylesheet(qt_api='pyqt5')
    app.setStyleSheet(style_sheet)
    myGUI = CustomMainWindow()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()

''''''
