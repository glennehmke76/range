-- terminal dump working wlab and range
sudo -u glennehmke pg_dump -t "^wlab*" birdata | psql -h birdlife.webgis1.com -p 5432 -U birdlife -d birdlife_birdata

sudo -u glennehmke pg_dump -t "^range*" birdata | psql -h birdlife.webgis1.com -p 5432 -U birdlife -d birdlife_birdata



5py2dk86YZ

does this do views?