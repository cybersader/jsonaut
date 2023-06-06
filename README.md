<!-- PROJECT LOGO -->
<p align="center">
  <!-- Add your project logo here -->
  <img src="https://github.com/cybersader/jsonaut/assets/106132469/bfc3ac6a-82f8-424d-be45-e62a220cd897" alt="Logo" width="200" height="200">

</p>

<!-- PROJECT TITLE -->
<h1 align="center">Jsonaut</h1>

<!-- PROJECT DESCRIPTION -->
<p align="center">
  An open source library and utility for exploring, analyzing, and flattening JSON files of any size into CSVs, along with CSV transformations, dynamic filtering, and all with low memory utilization.
</p>

<!-- Screenshots and Gifs -->

### Run GUI

The Run GUI is your command center for executing jobs based on the JSON files you've created. This powerful interface puts control in your hands, with clear indicators for job status and progress.

![jsonaut_yw3AwANiNb](https://github.com/cybersader/jsonaut/assets/106132469/64ff9b59-1885-44cb-b2ef-6a93fd277b49)

To use the Run GUI:
1. Launch Jsonaut, ensuring you are in 'Run GUI' mode.
2. Load the JSON configuration file you wish to execute.
3. Hit the 'Run' button and watch your job progress to completion!

### Builder GUI (Alpha) 🚧

The Builder GUI aids in the streamlined creation of JSON jobs config files.

> ❗ It currently lacks the ability to move jobs around, choose multiple variables, and easily configure a job's schema. However, you are encouraged to manually edit the JSON config files according to your needs. Manual editing does not require adding anything to the jobs_schema.json file.

![jsonaut_el5PHg64SI](https://github.com/cybersader/jsonaut/assets/106132469/157439f1-c61a-46f5-be5a-629999035088)

To use the Builder GUI:
1. Launch Jsonaut and click on 'Switch to Builder GUI' in the top menu bar.
2. Navigate through the menus to create your desired JSON configuration.
3. Save your file and it's ready for use with the Run GUI!
      - ❗ **Alternatively, edit a JSON file and prepend "config__" to the front so the "Run" GUI automatically detects it.**

⚠ Warning: because the Builder is not finished, and it requires editing a jobs_schema.json file, then it is usually better to create your own "config__<name here>.json" files.
___

## Uses of Jsonaut
- 📊 Data Engineering
    - Process any size CSV and JSON files (as long as it fits in local storage)
    - Transforming JSONs into CSVs and parsing it for imports into other apps
    - Looking through large CSV and JSON exports
    - Making it easier to apply Python-based code to data
    - Making pipelines that can easily be applied with a GUI
    - Sharing transformations with people without needing to understand Python or CLI (just download the exe)
- 🖥️ Cyber Security Activities
    - Integrating Security Products
    - Doing powerful processing on HUGE CSV and JSON files

## 🌟 Jsonaut Features, Design, Purpose

Jsonaut is designed to take LARGE (GBs of JSON data) and be able to apply Python transformations and processing.  It was originally made to "tabulize" or take JSON data and turn it into a CSV by granularly going through JSON objects. The "search and flatten" functionality was the original purpose of the program, but it involved to include numerous other transformations and a GUI to simplify the workflow.  Jsonaut uses the MVC architecture by utilizing JSON files to handle all of the jobs.

### **Why Jsonaut is special**

It can handle **LARGE JSON files** and has Python functions that aid in exploring the layout of these JSON files so that one can pick out the parts that they want to turn into a CSV and do so quickly and without error.

### Jsonaut Library
- 💉**Extensible** - it's easy to add a function, import it in the main.py file, then utilize it jobs.
- 🏎**Fast** - uses chunking and simple yet optimized functions to perform transformations on CSV and JSON files.
- 💻 **Local** - no need for external servers to work with HUGE JSON files

### GUI Features 🎁
- Can run with an executable and a python.exe (interpreter) file -- portable.
- Ability to run, build, and test in one place

## 🚀 Getting Started
To get a local copy of Jsonaut running, you can either use Git if you have it installed, or you can download the repository directly from GitHub.

### Download Executable from Releases (only requires Python for running jobs)
If you simply want to use Jsonaut without installing any prerequisites or going through any setup processes, you can download the latest executable directly from the Releases section in the GitHub repository.
- **Download Python 3.x:** Jsonaut is a Python-based library and requires a compatible Python version. [Download Python](https://www.python.org/downloads/)

### With Git Installed
1.  First, you need to clone the repository. If you don't have Git installed, you can download it from the [official Git website](https://git-scm.com/downloads).
2.  Open a terminal, navigate to the directory where you want to clone the repository and run:
    ```
    bashCopy codegit clone <repository-url>
    ```
3.  Proceed to the Installation & Setup section.

### Without Git
1.  Go to the [Jsonaut GitHub repo](https://github.com/cybersader/jsonaut).
2.  Click on the 'Code' button and then click 'Download ZIP'.
3.  Extract the downloaded ZIP file to your preferred location.
4.  Proceed to the Installation & Setup section.

### ⚙️ Installation & Setup
#### Prerequisites
Before installing Jsonaut, you need to ensure that you have the following prerequisites installed:
-   Python 3.x: Jsonaut is a Python-based library and requires a compatible Python version. [Download Python](https://www.python.org/downloads/)

#### Installing Jsonaut

1.  Navigate to the root directory of the cloned repository.
2.  If you're planning to use Jsonaut as a library, install the required Python packages using pip:
    ```
    pip install -r requirements.txt
    ```

### Setting up Jsonaut
1.  **Run the GUI.py file:** If you want to use Jsonaut's GUI, you can just run the `GUI.py` file in your Python environment.
2.  **Run main.py with config files:** If you prefer using the CLI, you can run Jsonaut's main functionality with the command: `python main.py --config "config__<text>.json"`. Replace `<text>` with your desired configuration.
3.  **Build the executable using setup.py:** If you want to build the Jsonaut executable, you can use the `setup.py` script. Running `python setup.py` will build the executable. If you want to run Jsonaut after building, use `python setup.py --run`.
4.  **Use the pre-built executable:** If you have downloaded the executable from the Releases section, you can just run it directly. No installation or Python environment setup is needed.

### 🛠️ IDEs for Python and Development
There are many IDEs you can use for Python development. Some of the popular ones are:
1.  [PyCharm](https://www.jetbrains.com/pycharm/)
2.  [Visual Studio Code](https://code.visualstudio.com/)
3.  [Atom](https://atom.io/)
4.  [Sublime Text](https://www.sublimetext.com/)
Select an IDE that suits your preference and comfort level. Each of these IDEs supports Python and should work with Jsonaut.

### Manual JSON Configuration for Jobs
If the Builder GUI does not support a specific feature you need, you can manually edit the JSON configuration files. The structure of the JSON files is intuitive, and you can use any text editor for this purpose. When creating a new job, you don't need to add it to the `jobs_schema.json` file unless you want to use the job with the Builder GUI.

#### Local Editors:
  -   [Visual Studio Code](https://code.visualstudio.com/): This is a feature-rich code editor that can handle JSON files with ease. It offers IntelliSense for auto-completing code and providing helpful information, as well as built-in Git commands. It also supports a wide array of plugins for enhancing functionality.
-   [Sublime Text](https://www.sublimetext.com/): This is a sophisticated text editor for code, markup, and prose. It has a slick user interface and exceptional performance. Sublime Text also has a rich ecosystem of plugins for extended functionalities.
-   [Atom](https://atom.io/): Atom is a hackable text editor for the 21st Century, built by GitHub. It's customizable and supports a vast library of plugins for added functionality. Atom also handles JSON files well.
-   [Notepad++](https://notepad-plus-plus.org/): This is a free (as in "free speech" and also as in "free beer") source code editor and Notepad replacement that supports several languages. It is lightweight and easy to use. Notepad++ provides color coding for JSON files, which can help with readability.
-   [Brackets](http://brackets.io/): This is a modern, open-source text editor that understands web design. It has several handy features like live preview and preprocessor support.
-   [JSONLint](https://jsonlint.com/): This is an online tool for validating, formatting, and analyzing JSON data. It's more suitable for smaller JSON files and quick edits.

#### Online Editors:
-   [JSON Editor Online](https://jsoneditoronline.org/): This is a web-based tool to view, edit, format, and validate JSON. It provides a tree view to navigate your data and a code editor for comfortable text editing.
-   [JSON Grid](https://www.jsongrid.com/): It's an easy-to-use JSON editor that offers a spreadsheet-like grid interface. It allows importing from and exporting to various formats, and it can create JSON schema as well.
-   [Online JSON Viewer](http://jsonviewer.stack.hu/): This is a simple tool that can parse JSON data. It offers a text editor mode, tree editor mode, and a viewer mode.
-   [Code Beautify JSON Editor](https://codebeautify.org/jsonviewer): A versatile tool that allows you to view, edit, and format JSON. It also has functionality to convert between JSON and other data formats.
-   [JSON Formatter & Editor](https://jsonformatter.org/): This online tool helps to format, validate, save, share, and edit JSON data.

