@echo off
dir /b "target\dpef-*.jar" > JAR
set /p JAR= < JAR
set JAR="target\%JAR%"
java -cp %JAR% org.webdatacommons.webtables.extraction.util.LocalWebTableExtractor %*