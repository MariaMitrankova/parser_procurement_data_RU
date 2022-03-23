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
            + https://github.com/aksyuk/zakupki_gov_ru/blob/master/filenames_description.md#:~:text=%D0%92%D1%81%D0%B5%2B%D1%81%D0%BF%D1%80%D0%B0%D0%B2%D0%BE%D1%87%D0%BD%D0%B8%D0%BA%D0%B8%2B%D0%BF%D0%BE%2B%D0%AD%D0%9F_v42%2B(10.3)%2B(%D0%94%D0%BE%D0%B1%D0%B0%D0%B2%D0%BB%D0%B5%D0%BD%2B%D0%AD%D0%9E%D0%9A).xlsx
            




   