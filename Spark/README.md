# PySpark Setup on macOS

## Prerequisites

- macOS 11+ (Big Sur or later)
- Python 3.8+

---

## 0. Install Homebrew (if not already installed)

Check if Homebrew is installed:

```bash
brew --version
```

If not found, install it:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

After installation, follow the printed instructions to add Homebrew to your PATH. On Apple Silicon (M1/M2/M3) Macs, add this to `~/.zshrc`:

```bash
export PATH="/opt/homebrew/bin:$PATH"
```

On Intel Macs, Homebrew installs to `/usr/local/bin` which is usually already in your PATH.

Reload your shell and verify:

```bash
source ~/.zshrc
brew --version
```

---

## 1. Install Java

PySpark requires Java 8, 11, or 17. Install via Homebrew:

```bash
brew install openjdk@17
```

Add Java to your PATH (add to `~/.zshrc` or `~/.bash_profile`):

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"
```

Reload your shell:

```bash
source ~/.zshrc
```

Verify:

```bash
java -version
```

---

## 2. Install PySpark

Install via pip:

```bash
pip install pyspark
```

Or inside a virtual environment (recommended):

```bash
python -m venv .venv
source .venv/bin/activate
pip install pyspark
```

Verify the installation:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

---

## 3. Configure Environment Variables

Add the following to `~/.zshrc` (or `~/.bash_profile`):

```bash
export SPARK_HOME=$(python -c "import pyspark; import os; print(os.path.dirname(pyspark.__file__))")
export PATH="$SPARK_HOME/bin:$PATH"
export PYSPARK_PYTHON=python3
```

Reload:

```bash
source ~/.zshrc
```

---

## 4. Run Your First Spark Program

```bash
python Spark/101/first_spark_program.py
```

Or launch an interactive PySpark shell:

```bash
pyspark
```

---

## 5. Verify Spark UI

When a Spark job is running, the Spark Web UI is available at:

```
http://localhost:4040
```

---

## Troubleshooting

### `JAVA_HOME` not set
If you see `JAVA_HOME is not set`, re-run:
```bash
export JAVA_HOME=$(/usr/libexec/java_home)
```

### Port 4040 already in use
Spark will automatically try the next available port (4041, 4042, ...). Check terminal output for the actual URL.

### `Py4JJavaError` on Apple Silicon (M1/M2/M3)
Ensure you installed the ARM-native version of OpenJDK via Homebrew:
```bash
brew install openjdk@17
```
Avoid Rosetta-based Java installations.

### Permission denied on `/tmp/hive`
```bash
sudo mkdir -p /tmp/hive
sudo chmod 777 /tmp/hive
```

---

## Project Structure

```
Spark/
├── 101/
│   └── first_spark_program.py      # Basic Spark setup and usage
└── rdd_vs_dataframe/
    └── rdd_dataframe.py            # RDD vs DataFrame examples
```

---

## Dependencies

| Package  | Purpose              |
|----------|----------------------|
| pyspark  | Apache Spark for Python |
| openjdk  | JVM runtime for Spark   |
