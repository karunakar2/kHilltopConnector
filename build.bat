@echo off
title "Build the package"

:: uninstall existing packages to make sure nothing crashes
pip uninstall kHilltopConnector

:: build new version
py -m build

:: install the fresh update
cd dist/
pip install kHilltopConnector-0.0.1-py3-none-any.whl

pause
echo "press key to exit"
exit