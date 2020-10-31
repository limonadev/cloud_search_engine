export GOOGLE_APPLICATION_CREDENTIALS="My Project-3778cd48121b.json"
rm -r Rank
rm -r Index
mkdir -p Rank/output
mkdir -p Index/output_index
python storage_getter.py