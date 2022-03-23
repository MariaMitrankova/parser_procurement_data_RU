# parser_procurement_data_RU
 Parser for ftp-server of russian procurement

This parser provides code for collecting data about Russian procurement since 2014 from ftp-server ftp://free:free@ftp.zakupki.gov.ru.
The main code was written by Dmitry Ivanov, aspirant in Game Theory and Decision Making International Laboratory of St.Petersburg HSE and updated by me, research assistant in the lab.

* Files:
    + create_tables - creates database of procurement data
    + etl - collects zip files from ftp-server, unpacks them and then unites them and starts parsing consequently
    + sql - some useful functions for updating database
    + transform - code for parsing xml-files and inserting data into database
    + update - main file uniting all of the code; to start parsing open in terminal and type "python update.py all"
    + utils - some additional useful functions 
    
* Helpful notes on file names:
    + https://zakupki.gov.ru/epz/main/public/download/downloadDocument.html?id=33287
    + https://clck.ru/Ycx7H
    + https://docs.google.com/document/d/14QeZPaOvUmwK3_ZKwscgGaGDNe6cb2lz/edit?usp=sharing&ouid=103180529918038360903&rtpof=true&sd=true
            
            




   