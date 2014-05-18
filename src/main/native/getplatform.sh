platform="`uname | tr [A-Z] [a-z]`"

if [[ $platform == "linux" ]]; then
    echo "linux"
elif [[ $platform == "darwin" ]]; then
    echo "darwin"
elif [[ $platform == cygwin* ]]; then
    echo "cygwin"
fi
