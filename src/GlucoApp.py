#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on Thu Sep 30 18:57:04 2021

@author: Jyothish KP
"""
import sys
import argparse
from pipeline.SimplePipeline import *

def parse_input_params(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("-p","--pipeline_name", help="Pipeline config name")
    args = parser.parse_args()
    return (args.pipeline_name,)

def main():
    print("Running '{}'".format(__file__))
    (pipeline_name,)= parse_input_params(sys.argv)
    print (f"pipeline_name = {pipeline_name}")
    service_request(pipeline_name)
    
    print("Processing Comppleted")
    
    
main()
    
