/*

===================================================
Create Database and Schemas
===================================================
Script Purpose:
    This script creates a new databases name'DataWearhouse' after checking if it alread exict.
    If the database exists, it is dropped and recreated. Additionally, the project sets up three schemas    
    within  the database : 'Bronze', 'Sliver', 'Gold'.

WARNING:
   Running this script will drop the entire 'DataWearhous' database if it exists.
   All data in the database will be permanently deleted. Procced with caustion
   and ensure you have proper backups before running this script. 
*/

USE Mmaster ;
GO
-- Drop and recreate the 'DataWearhous' database
