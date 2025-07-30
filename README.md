# **Asynchronous Processing**

This repository contains two Python scripts for automating file management and text data analysis:

1. **`task1_copy_sort.py`**: An asynchronous file sorter that organizes files from a source directory into a target directory, grouped by file extension.
2. **`task2_word_freq.py`**: A word frequency analyzer that downloads text from a URL, processes it using a parallel MapReduce approach, and visualizes the results.

## **Table of Contents**

- [Key Features](#key-features)
- [Requirements and Installation](#requirements-and-installation)
- [Usage](#usage)
  - [Task 1: Asynchronous File Sorter](#1-asynchronous-file-sorter)
  - [Task 2: Word Frequency Analyzer](#2-word-frequency-analyzer)

## **Key Features**

### **1. Asynchronous File Sorter (`task1_copy_sort.py`)**

- **Asynchronous Copying**: Uses `asyncio` and `aiofiles` for efficient, non-blocking processing of large numbers of files.
- **Recursive Search**: Automatically discovers files in all subdirectories of the source directory.
- **Extension-Based Sorting**: Creates subdirectories in the target directory for each file extension (e.g., `.txt`, `.jpg`). Files without extensions are placed in a `no_extension` folder.
- **Duplicate Handling**:
  - **Identical Files**: Skips copying if a file with the same name and content (verified by SHA-256 hash) already exists.
  - **Different Content**: Files with the same name but different content are saved with a short hash appended to the name (e.g., `document_a1b2c3d4.pdf`).
- **Concurrent Task Limits**: Uses a semaphore to control the number of simultaneous file operations, preventing system overload.
- **Logging**: Records all actions, including copied files, skipped duplicates, renamed files, and errors.

### **2. Word Frequency Analyzer (`task2_word_freq.py`)**

- **Text Download**: Retrieves text content directly from a specified URL.
- **Efficient Processing**: Employs a parallel MapReduce approach to analyze large texts with minimal memory usage.
- **Parallelization**: Supports multithreading (`threads`) or multiprocessing (`processes`) modes for faster computation.
- **Automatic Language Detection**: Identifies the text's language using `langdetect` and removes stop words (common words like articles or prepositions) via `stopwords-iso`.
- **Flexibility**:
  - Allows custom stop-word lists via a file.
  - Supports analyzing only specific words with the `--words` option (disables stop-word removal).
- **Visualization and Export**:
  - Generates a horizontal bar chart of the top-N most frequent words using `matplotlib` (displayed in an interactive window or saved as PNG).
  - Exports the full frequency table to a `.csv` file.
- **Performance Report**: Provides execution time, word count statistics, and, if `psutil` is installed, memory and CPU usage.

## **Requirements and Installation**

1. **Python 3.8+**.
2. Create a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv/Scripts/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   **Note**: The `psutil` library is optional for `task2_word_freq.py`. The script functions without it but will not provide memory or CPU usage statistics.

## **Usage**

### **1. Asynchronous File Sorter**

The script accepts a source directory path and, optionally, a target directory path. If no target is specified, a `dist` folder is created in the current directory.

**Syntax**:
```bash
python task1_copy_sort.py <source/path> [destination/path]
```

**Example**:
```bash
# Sort files from the "Source" folder into a "Output" folder
python task1_copy_sort.py ./Source ./Output
```

### **2. Word Frequency Analyzer**

The script requires a text URL and supports optional flags for customizing the analysis.

**Syntax**:
```bash
python task2_word_freq.py <URL> [options]
```

**Examples**:
1. **Basic Analysis**: Download text and display a chart of the 10 most frequent words.
   ```bash
   python task2_word_freq.py "https://gutenberg.net.au/ebooks01/0100021.txt"
   ```
2. **Advanced Analysis**: Analyze the top 20 words using 4 CPU cores, save the chart to `nineteen_eighty-four_top20.png`, and export the frequency table to `full_freq.csv`.
   ```bash
   python task2_word_freq.py "https://gutenberg.net.au/ebooks01/0100021.txt" --top 20 --backend processes --png nineteen_eighty-four_top20.png --csv full_freq.csv
   ```
3. **Specific Words Analysis**: Count the frequency of the words `war`, `peace`, and `love`.
   ```bash
   python task2_word_freq.py "https://gutenberg.net.au/ebooks01/0100021.txt" --words war peace love
   ```

**Options**:

| Flag                   | Description                                                         |
|------------------------|---------------------------------------------------------------------|
| `--top N`              | Number of top words to visualize (default: 10).                     |
| `--backend {threads,processes}` | Parallelization mode: `threads` (default) or `processes`. |
| `--png <path>`         | Save the chart as a PNG file.                                       |
| `--csv <path>`         | Save the frequency table as a CSV file.                             |
| `--words <word1> ...`  | Analyze only specified words (disables stop-word removal).          |
| `--stopwords-file <path>` | Path to a custom stop-words file (one word per line).            |
| `--no-auto-stopwords`  | Disable automatic language detection and stop-word removal.         |