
# Analysis of GitHub repositories using Grimoire tools

## Installing Python dependencies

Let's assume we want to install dependencies in /tmp/pip:

pip install --target=/tmp/pip rpy2

For this to work, the proper PYTONPATH has to be set up (assuming pip was run with python2.7:

export PYTHONPATH=/tmp/pip/:$PYTHONPATH

