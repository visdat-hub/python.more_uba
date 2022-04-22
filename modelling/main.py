# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import json
import sys
import time

from general.process import ProcessStbP

if __name__ == "__main__":

    print ("Starte STOFFBILANZ-Modellierung ...")

    t1 = time.time()

    # Konfigurationsobjekt ladeb

    if len(sys.argv) == 2:
        if sys.argv[1]:
            pathConfigFile = sys.argv[1]
            
            print('--->',pathConfigFile)
            json_file = open(pathConfigFile)
            configObject = json.load(json_file)
            print('--->',configObject)
            #sys.exit()
            
            
    else :
        print ("Bitte Configfile angeben!")
        
    processP = ProcessStbP(configObject)
    processP.baseProcessing(configObject)

    t2 = time.time()
    print ('Laufzeit:')
    print (str(t2 - t1))
