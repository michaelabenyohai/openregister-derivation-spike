# openregister-derivation-spike
A spike for openregister java derivations.

```cat {register}.rsf | java -jar build/libs/rsf-parser.jar {indexName} {indexRender} {registerVersion} {indexValue}```

where `registerVersion` and `indexValue` are optional.

- `indexName` is the name of the index - any of "record", "current-countries", "local-authority-by-type"
- `indexRender` displays the index in different ways - "current" (current view), "entries" (the RSF), "indexTable" (the whole state stored)
- `registerVersion` is an entry number in the original register, at which to open the derivation
- `indexValue` is a value of the index - "NMD" is an `indexValue` for "local-authority-by-type"

e.g. 

```cat country.rsf | java -jar build/libs/rsf-parser.jar current-countries current```

e.g. 

```cat local-authority-eng.rsf | java -jar build/libs/rsf-parser.jar local-authority-by-type entries | java -jar build/libs/rsf-parser.jar record current 10 | gxargs -L 1 ./pretty-print-rsf.sh```

e.g. 

```cat country.rsf | java -jar build/libs/rsf-parser.jar current-countries indexTable```
