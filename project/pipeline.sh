#!/bin/bash
DIR_script=$( dirname -- "$( readlink -f -- "$0"; )"; )

install_pip () {
	echo -e "\033[0;35minstall or upgrade pip\033[0m"
	pip --version 2> /dev/null
	if [ "$?" = "0" ];then
		pip install --upgrade pip
		if [ "$?" = "0" ];then
			echo -e "\033[0;32mpip was successfully upgraded\033[0m"
		else
			echo -e "\033[0;33mpip upgrade was failed and use current pip version\033[0m"
			
		fi
	else
		curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
		check_kernel=$(uname | grep WIN)
		if [[ -z $check_kernel ]]; then
			python get-pip.py
		else
			py get-pip.py
		fi
		pip_version=$(pip --version)
		if [[ -n $pip_version ]]; then
			echo -e "\033[0;32m$pip_version was successfully installed\033[0m"
		else
			echo -e "\033[0;31mpip installation was failed\033[0m"
		fi
	fi
}
install_python() {
	echo -e "\033[0;35minstall python\033[0m"
	python3 -V 2> /dev/null
	if [ "$?" = "0" ];then
		echo -e "\033[0;32mpython was already installed\033[0m"
		install_pip
	else
		echo "install python from source"
		git clone https://github.com/python/cpython.git
		cd cpython
		./configure
		make
		make test
		sudo make install
		python_version=$(python3 -V)
		if [[ -n $python_version ]]; then
			echo -e "\033[0;32m$python_version was successfully installed\033[0m"
			install_pip
		else
			echo -e "\033[0;31mPython installation was failed\033[0m"
			exit 1
		fi
	fi
}
install_panda() {
	echo -e "\033[0;35minstall panda\033[0m"
	pip install pandas
}
run_project() {
        cd $DIR_script
        python3 Project3.py
}

####RUN Script####
install_python
install_panda
run_project

