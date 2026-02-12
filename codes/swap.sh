#!/bin/bash

sudo swapoff /swap.img 

sudo rm /swap.img 

sudo fallocate -l 64G /swap.img 

sudo chmod 600 /swap.img 

sudo mkswap /swap.img 

sudo swapon /swap.img 

echo "Swap file has been updated to 16G."
