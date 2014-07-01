platform="`uname -m`"

if [[ $platform == "x86_64" ]]; then
    echo "64"
elif [[ $platform == "i686" ]]; then
    echo "32"
fi
