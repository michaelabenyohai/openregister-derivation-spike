In each of the directories there are three files.

- register.rsf (RSF for the orginal register)
- index.rsf (RSF produces by appying the named index function on register.rsf)
- index-table.tsv (a representation of the internal index table that produces index.rsf from register.rsf)

To load index-table.tsv into an index table use the following command:

`cat index-table.tsv | psql -c "COPY index FROM STDIN NULL 'null'" {database-name}`
