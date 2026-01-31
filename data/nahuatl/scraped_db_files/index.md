# Context of this directory

In this directory lives the db files used with SQLite and DB Browser that hold the
data we scraped from the <https://nahuatl.wired-humanities.org/> website. This
accompanies the data that was initially given to us from that project but that we
found lacking compared to what they had in their actual website.

nahuatl_scraped.db has the scraped data in relational tables. nahuatl_backup.db is a backup
just in case but might be out of date. look at [schema.sql](schema.sql) for more context of the
schema of the relational tables used to store the data scraped.
