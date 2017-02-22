if [[ $1 == append-entry ]]
then
  echo $1\\t$2\\t$4
  echo $3 | sed -e $'s/\[/\\\t[/g' | sed -e $'s/,/\\\n\\\t/g'
else
  echo $1\\t$2
fi
