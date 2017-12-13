package org.webdatacommons.structureddata.util;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCRecord;

/**
 * Created by ppetrovs on 4/15/14.
 */
public class WarcToTxt {

    private static Logger log = Logger.getLogger(WarcToTxt.class);

    public static void main(String[] args){
        log.info("Converting to text..");
        String arcFileName = args[0];
        String outFilename = args[0];
        StringBuilder sb = new StringBuilder();

        int fileCounter = 1;
        int recordCounter = 0;
        boolean fileFlag = false;
        try{
            ReadableByteChannel gzippedArcFileBC = Channels.newChannel(new DataInputStream(new FileInputStream(arcFileName)));
            final ArchiveReader reader = ArchiveReaderFactory.get(arcFileName, Channels.newInputStream(gzippedArcFileBC), true);
            log.info("Archive reader ready!");
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter( new GZIPOutputStream(new FileOutputStream(outFilename+".0.txt.gz"))));
            Iterator<ArchiveRecord> readerIt = reader.iterator();
            log.info("Iterating trough the reader...");
            while (readerIt.hasNext()) {
                if(recordCounter % 1000 == 0 && fileFlag){
                   writer.flush();
                   writer.close();
                   writer = new BufferedWriter(new OutputStreamWriter( new GZIPOutputStream(new FileOutputStream(outFilename+"."+fileCounter+".txt.gz"))));
                   fileCounter++;
                }
                ArchiveRecord record = readerIt.next();
                ArchiveRecordHeader header = record.getHeader();
                if (!header.getMimetype().equals("application/http; msgtype=response")) {
                    fileFlag=false;
                    continue;
                }
                if(header.getUrl().contains("11freunde")){
                WARCRecordUtils.getHeaders(record, true);
                sb.append("URL:\n");
                sb.append(header.getUrl()).append("\n");
                sb.append("CONTENT:\n");
                sb.append(WARCRecordUtils.convertToText((WARCRecord) record)).append("\n");
                recordCounter++;
                writer.append(sb);
                sb.setLength(0);
                fileFlag=true;
                }
            }
            writer.flush();
            writer.close();
            log.info("Finished");
        }
        catch (FileNotFoundException fe){
            log.error("File not found", fe);
        }
        catch (IOException e){
            log.error("Could not create archive reader", e);
        }
    }
}
