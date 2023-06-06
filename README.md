<!-- PROJECT LOGO -->
<p align="center">
  <!-- Add your project logo here -->
  <img src="https://github.com/cybersader/jsonaut/assets/106132469/bfc3ac6a-82f8-424d-be45-e62a220cd897" alt="Logo" width="200" height="200">

</p>

<!-- PROJECT TITLE -->
<h1 align="center">Jsonaut</h1>

<!-- PROJECT DESCRIPTION -->
<p align="center">
  A library and utility for exploring, analyzing, and flattening JSON files of any size into CSVs, along with CSV transformations, dynamic filtering, and all with low memory utilization. This is a workflow for running Python locally for data engineering tasks and was originally meant for integrating security tool data.
</p>

<!-- Screenshots and Gifs -->

### Run GUI

The Run GUI is your command center for executing jobs based on the JSON files you've created. This powerful interface puts control in your hands, with clear indicators for job status and progress.

![jsonaut_Lef7SnXuZD](https://github.com/cybersader/jsonaut/assets/106132469/972ea1ce-b72a-4290-a3a8-fd9d4cdf88d1)

To use the Run GUI:
1. Launch Jsonaut, ensuring you are in 'Run GUI' mode.
2. Load the JSON jobs configuration file you wish to execute.  Look at [Adding Jobs to the Code](#adding-jobs-to-the-code🤩🐍) section for making your own Python jobs. 
3. Hit the 'Run' button and watch your job progress to completion!

### Builder GUI (Alpha) 🚧

The Builder GUI aids in the streamlined creation of JSON jobs config files.

> ❗ It currently lacks the ability to move jobs around, choose multiple variables, and easily configure a job's schema. However, you are encouraged to manually edit the JSON config files according to your needs. Manual editing does not require adding anything to the job_schema.json file.

![jsonaut_el5PHg64SI](https://github.com/cybersader/jsonaut/assets/106132469/157439f1-c61a-46f5-be5a-629999035088)

To use the Builder GUI:
1. Launch Jsonaut and click on 'Switch to Builder GUI' in the top menu bar.
2. Navigate through the menus to create your desired JSON configuration.
3. Save your file and it's ready for use with the Run GUI!
      - ❗ **Alternatively, edit a JSON file and prepend "config__" to the front so the "Run" GUI automatically detects it.**

⚠ Warning: because the Builder is not finished, and it requires editing a job_schema.json file, then it is usually better to create your own "config__<name here>.json" files.
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
    git clone https://github.com/cybersader/jsonaut
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
#### There are 4 ways to setup and use Jsonaut (Look at below Usage section for more detail):
1.  **Run the GUI.py file:** If you want to use Jsonaut's GUI, you can just run the `GUI.py` file in your Python environment.
2.  **Run main.py with config files:** If you prefer using the CLI, you can run Jsonaut's main functionality with the command: `python main.py --config "config__<text>.json"`. Replace `<text>` with your desired configuration.
3.  **Build the executable using setup.py:** If you want to build the Jsonaut executable, you can use the `setup.py` script. Running `python setup.py` will build the executable. If you want to run Jsonaut after building, use `python setup.py --run`.
4.  **Use the pre-built executable:** If you have downloaded the executable from the Releases section, you can just run it directly. No installation or Python environment setup is needed.

# Usage
  
## Running Python Jobs
**Jsonaut** is flexible and can be used in different scenarios depending on your specific requirements. Here are some typical use cases:
#### 1\. Running Predefined Jobs with the GUI 🖥️

To execute predefined jobs using the Jsonaut GUI, you will require a Python interpreter. Here are the ways to proceed based on your setup:
-   **Python Installed:** If you already have Python installed on your machine, Jsonaut will automatically utilize the installed Python interpreter. Simply download the ready-to-use Jsonaut executable from the [Releases](https://github.com/cybersader/jsonaut/releases) section. This standalone executable doesn't require additional installations. Just run it, and the Jsonaut GUI will spring up!
-   **No Python Installed:** If you don't have Python installed, you'll need to select a Python interpreter executable when you run Jsonaut. This could be a standalone Python interpreter executable that you have downloaded separately. You can download the official Python interpreter executable from the [Python Downloads](https://www.python.org/downloads/) page.    
-   **Running from Source Code:** Another option is to run the GUI directly from the source code. Clone or download the project and run the `GUI.py` file in your Python environment.   
Remember that for all these options, you'll need to have the job definitions you want to run available in the correct JSON format. Use one of the recommended JSON editors to make this process easier. Happy running! 👍

#### 2\. Running Jobs from the Command Line 📜
For those who prefer a command line interface or need to integrate Jsonaut into other scripts or workflows, you can directly use Jsonaut's main functionality from the command line.
-   Download the project and run the command: `python main.py --config "config__<text>.json"`. Replace `config__<text>` with your specific JSON configuration file.

#### 3\. Building Your Own Executable 🛠️
If you want to distribute your own version of Jsonaut or customize the build process, you can use the `setup.py` script provided.
-   Download the project and run `python setup.py`. This will build the executable.
-   If you want to run Jsonaut immediately after building, use `python setup.py --run`.

#### 4\. Developing Your Own Transformations with Python 🐍
For advanced users who wish to extend Jsonaut with their own transformations or features, you can utilize the power of Python and the extensibility of the Jsonaut architecture.
-   Download the project and import the Jsonaut library in your Python code.
-   Develop your custom transformations following the same structure used in Jsonaut's built-in transformations.
-   Run your transformations through the Jsonaut system by either using the GUI or the command line.

Remember to consult the detailed documentation and examples provided to understand how Jsonaut works and how to effectively extend its functionalities.

## Make Your Own Python Jobs 🐍 ◀◀◀◀◀◀◀
**Jsonaut is extensible - 3 steps needed to fully add your own jobs:**
1. Add a Python function to the code (I added the current ones to `utils.py`)
2. `import` the function in `main.py`
3. Add it to a config file
4. (optional) add it to the `job_schema.json` to work with the Builder GUI (alpha)
5. Run it (Usage section above)

### Prereqs for Editing Jobs (Software)

#### 🛠️ IDEs for Python Development
There are many IDEs you can use for Python development. Some of the popular ones are:
1.  [PyCharm](https://www.jetbrains.com/pycharm/)
2.  [Visual Studio Code](https://code.visualstudio.com/)
3.  [Atom](https://atom.io/)
4.  [Sublime Text](https://www.sublimetext.com/)
Select an IDE that suits your preference and comfort level. Each of these IDEs supports Python and should work with Jsonaut.

#### Manual JSON Configuration for Jobs
If the Builder GUI does not support a specific feature you need, you can [manually edit the JSON configuration files](#configuring-json-files). The structure of the JSON files is intuitive, and you can use any text editor for this purpose. When creating a new job, you don't need to add it to the `job_schema.json` file unless you want to use the job with the Builder GUI.

##### Local Editors:
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

## Adding Jobs to the Code 🤩🐍

### Making JSON Job Config Files
Jsonaut leverages JSON files to define jobs and their parameters. An example of a simple configuration JSON file is:
```
{
    "jobs": [
        {
            "name": "example_job",
            "type": "example_job_type",
            "input_csv": "example.csv",
            "example_parameter": ["item_1", "item_2"]
        }
    ]
}

```
In the "jobs" array, each object represents a job that Jsonaut will execute. 
Required fields in a `job` object:
- `name` - recognizable name for that step/job
- `type` - how Jsonaut runs the applicable function (checks job type in main.py)

### Adding Jobs Example

When adding new jobs to the code in `main.py`, you will need to handle each job's parameters individually. You can use the `get` method on the job dictionary object to fetch these parameters. `get` is a safer way to access a dictionary's value since it won't cause an error if the key doesn't exist; it will simply return `None`.

First, let's define a new function in a new Python file that performs a specific job. This function takes parameters that configure how the job should be done. Here's an example:
```
# File: my_jobs.py (Jsonaut uses utils.py, but you can use whatever you want)

def my_new_job(input_csv, column_name, new_value):
    # logic to read CSV, change a specific column's value to 'new_value', and save the file.

```
After creating your function in a separate file, you can import it to `main.py` and then define a new job type:

```
# main.py file

# import your function
from my_jobs import my_new_job

# ...

# inside main() function

# where the jobs are attached to jsonaut
for job_index, job in enumerate(jobs):
    job_name = job.get("name")

    job_matched = False

    if job.get("type") == "my_new_job":
        input_csv = job.get("input_csv")
        column_name = job.get("column_name")
        new_value = job.get("new_value")

        # call your job function
        my_new_job(input_csv, column_name, new_value)

```

You can add the new job in the config JSON file:
```
{
    "jobs": [
        {
            "name": "my_new_job_1",
            "type": "my_new_job",
            "input_csv": "example.csv",
            "column_name": "asset_category",
            "new_value": "new_category"
        }
    ]
}
```

## Updating the Job Schema ⚠(only if you use the Builder GUI)(STILL IN ALPHA)
The `job_schema.json` file is a crucial part of the configuration, as it defines the structure and parameters for each job. Let's look at how we can add and modify job definitions in this file.
  
Each job in the `job_schema.json` file is defined as a JSON object in the `job_schema` array. Every job object should have the following properties:
-   `type`: This is the name of the job, which should match the function name in your code.
-   `default_name`: A friendly name for the job that gets filled in the builder widget when the job is selected.
-   `input_param`: The main input parameter for the job. This tells the builder widget which parameter is the primary one.
-   `input_match`: A regular expression that validates the type of file that can match the `input_param`.
-   `params`: An object that describes each parameter that the job requires. This is a key component of the job schema.

The `params` object in turn consists of the following properties for each parameter:
-   `type`: Specifies the data type of the parameter. It can be `file`, `list`, `dict`, `int`, `float`, `num`, or `number`.

For parameters of `dict` type, there are a couple of additional properties:
-   `options`: Descriptions for the keys in the dictionary. These descriptions are displayed when editing the object in the Builder.
-   `default`: The default value for the parameter. This is the value that gets loaded into the Builder when the job type is selected.

And finally, every job object also has:
-   `output_prepends`: A string that gets prepended to the output filename.
-   `output_ext`: The extension of the output file.

Remember, when adding a new job or modifying an existing one, the `type` in the `job_schema.json` file should match the function name in your code, and the parameter names in the `params` object should match the parameters in your function definition.

After updating the `job_schema.json` file, you can then use the updated schema in the JSON config file to define and configure the jobs that should be executed.

Here's an example of how a job object might look in the `job_schema.json` file:
```
{
  "type": "rename_columns",
  "default_name": "rename_columns",
  "input_param": "input_csv",
  "input_match": ".+\\.csv$",
  "params": {
    "input_csv": {
      "type": "file"
    },
    "rename_obj": {
      "type": "dict",
      "options": {
        "keys": "Old column names (exact matches)",
        "values": "New column names (exact matches)"
      },
      "default": {}
    },
    "similarity_threshold": {
      "type": "float",
      "default": 0.9
    }
  },
  "output_prepends": "{input_csv_name}_newjob__",
  "output_ext": "csv"
}
  
```
The rename_columns job defined above takes an input_csv file and a rename_obj dictionary as input, and produces a CSV file with renamed columns.
