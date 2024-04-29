@echo off
rem Set the path to the Rscript executable
set RSCRIPT="C:\Program Files\R\R-4.3.1\bin\Rscript.exe"
rem Set the path to the R script to execute
set RSCRIPT_FILE="C:\QMS\Adeno VMISC\adeno_misc.R"
rem Execute the R script
%RSCRIPT% %RSCRIPT_FILE%
rem Pause so the user can see the output
exit