package com.taomee.bigdata.transfer;

import java.util.LinkedHashSet;
import java.util.Iterator;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

public class TransferMain {
    LinkedHashSet<String> fileLists;
    TransferFrom from;
    TransferTo   to;

    public void run(String[] argv) {
        fileLists = new LinkedHashSet<String>();
        for(int i=0; i<argv.length; i++) {
            if(!argv[i].startsWith("-")) {
                fileLists.add(argv[i]);
            } else {
                i++;
            }
        }
        from = new TransferFromMultiStat();
        to   = new TransferToSource();
        from.conf(argv);
        to.conf(argv);

        Iterator<String> it = fileLists.iterator();
        while(it.hasNext()) {
            try {
                String file = it.next();
                BufferedReader reader = new BufferedReader(new FileReader(file));
                String line;
                while((line = reader.readLine()) != null) {
                    StatInfo statInfo = from.transferLine(line);
                    if(statInfo != null) {
                        System.out.println(to.transferStat(statInfo));
                    }
                }
                reader.close();
            } catch (FileNotFoundException e) {
                System.err.println(e.getMessage());
            } catch (IOException e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] argv) {
        (new TransferMain()).run(argv);
    }
}
