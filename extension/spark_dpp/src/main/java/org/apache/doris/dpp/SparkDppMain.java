// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.dpp;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

public class SparkDppMain {
    private static final Logger LOG = LogManager.getLogger(SparkDppMain.class);
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: SparkDppMain <path_to_jobconf>");
            System.exit(1);
        }
        String configPath = args[0];
        String config = "";
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            URI uri = new URI(configPath);
            fs = FileSystem.get(uri, conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        BufferedReader br = null;
        try {
            StringBuilder sb = new StringBuilder();
            Path srcPath = new Path(configPath);
            InputStream in = fs.open(srcPath);
            br = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            config = sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                br.close();
            }
        }
        SparkDpp sparkDpp = new SparkDpp();
        boolean ret = sparkDpp.parseConf(config);
        if (!ret) {
            System.exit(1);
        }
        sparkDpp.doDpp();
    }
}
