/**
 * The contents of this file may be used under the terms of the Apache License, Version 2.0
 * in which case, the provisions of the Apache License Version 2.0 are applicable instead of those above.
 *
 * Copyright 2014, Ecarf.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.ecarf.core.compress;

import io.ecarf.core.utils.Constants;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2Utils;
import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * Processes a normal/gzip input file and outputs
 * to a gzip file
 * 
 * @author Omer Dawelbeit (omerio)
 * FIXME rename to NxProcessor
 */
public class NxGzipProcessor {

    private final static Log log = LogFactory.getLog(NxGzipProcessor.class);

    private String inputFile;

    private String outputFile;

    /**
     * 
     * @param inputFile
     */
    public NxGzipProcessor(String inputFile) {
        this(inputFile, null);
    }

    /**
     * @param inputFile
     * @param outputFile
     */
    public NxGzipProcessor(String inputFile, String outputFile) {
        super();
        this.inputFile = inputFile;
        
        // no output file is provided, then workout a suitable filename
        if(StringUtils.isBlank(outputFile)) {
            // get the file name before the ext
            String ext = FilenameUtils.getExtension(inputFile);
            // construct an output file in the format inputfile_out.ext
            this.outputFile = StringUtils.removeEnd(inputFile, "." + ext);	
            this.outputFile = this.outputFile + Constants.OUT_FILE_SUFFIX + ext;
        
        } else {
            this.outputFile = outputFile;
        }
    }

    /**
     * Read the input file, gunziped if needed and call the callback to process each line
     * no output is produced
     * @param callback
     * @throws IOException
     */
    public void read(NxGzipCallback callback) throws IOException {

        try(BufferedReader deflated = new BufferedReader(new InputStreamReader(
                this.getDeflatedInputStream(new FileInputStream(this.inputFile))), Constants.GZIP_BUF_SIZE);) {
            
            NxParser nxp = new NxParser(deflated);

            while (nxp.hasNext())  {

                Node[] ns = nxp.next();

                if (ns.length == 3) {
                    //We are only interested in triples, no quads
                    callback.processNTriple(ns);

                } else {
                    //log.warn("Ignoring line: " + ns);
                    callback.processNQuad(ns);
                }
            }
        }
    }

    /**
     * Reads the input file, gunziped if needed, calls the callback to process
     * each line that being read then writes the file back to a gziped output file
     * @param callback
     * @throws IOException 
     */
    public String process(NxGzipCallback callback) throws IOException {

        try(BufferedReader deflated = new BufferedReader(new InputStreamReader(
                this.getDeflatedInputStream(new FileInputStream(this.inputFile))), Constants.GZIP_BUF_SIZE);) {


            try(PrintWriter writer = new PrintWriter(new BufferedOutputStream(
                    this.getInflatedOutputStream(new FileOutputStream(this.outputFile)), Constants.GZIP_BUF_SIZE));) {
                
                String outLine;

                callback.setOutput(writer);

                NxParser nxp = new NxParser(deflated);

                while (nxp.hasNext())  {

                    Node[] ns = nxp.next();

                    //We are only interested in triples, no quads
                    if (ns.length == 3) {

                        outLine = callback.processNTriple(ns);
                        if(outLine != null) {
                            writer.println(outLine);
                        }

                    } else {
                        //log.warn("Ignoring line: " + ns);
                        
                        outLine = callback.processNQuad(ns);
                        if(outLine != null) {
                            writer.println(outLine);
                        }
                    }
                }

            }

            return this.outputFile;
        }
    }

    /**
     * Get a deflated stream from the provided input
     * @param input
     * @return
     * @throws IOException
     */
    private InputStream getDeflatedInputStream(InputStream input) throws IOException {

        InputStream deflated = input;

        // gzip
        if(GzipUtils.isCompressedFilename(this.inputFile)) {
            deflated = new GZIPInputStream(input, Constants.GZIP_BUF_SIZE);

        } 
        // bz2
        else if(BZip2Utils.isCompressedFilename(this.inputFile)) {
            deflated = new BZip2CompressorInputStream(new BufferedInputStream(input));
        }

        return deflated;
    }
    
    /**
     * Get inflated output stream form the provided output stream
     * @param output
     * @return
     * @throws IOException
     */
    private OutputStream getInflatedOutputStream(OutputStream output) throws IOException {
        OutputStream inflated = output;
        
        // gzip
        if(GzipUtils.isCompressedFilename(this.inputFile)) {
            inflated = new GZIPOutputStream(output, Constants.GZIP_BUF_SIZE);

        } 
        // bz2
        else if(BZip2Utils.isCompressedFilename(this.inputFile)) {
            inflated = new BZip2CompressorOutputStream(new BufferedOutputStream(output));
        }
        
        return inflated;
    }

    /**
     * @param inputFile the inputFile to set
     */
    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    /**
     * @param outputFile the outputFile to set
     */
    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

}
