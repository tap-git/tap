/*
 * Licensed to Think Big Analytics, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Think Big Analytics, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Copyright 2010 Think Big Analytics. All Rights Reserved.
 */
package tap.core;

import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.conf.Configured;
import tap.util.CacheUtils;

public class TapMapper<IN,OUT> extends Configured implements TapMapperInterface<IN,OUT> {

	@Override
	public void init(String path) {
		// TODO Auto-generated method stub
	}
	
    /**
     * To be used by map method to access mapper parameters stored serialized into the Distributed Cache.
     * @return The Distributed Cache object cast as a Serializable
     * @throws ClassNotFoundException 
     * @throws IOException 
     */
    protected Serializable getMapperParameter(String key) {
    	try {
			return (Serializable) CacheUtils.getFromCache(key, getConf());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return null;
    }

    @SuppressWarnings("unchecked")
    public void map(IN in, Pipe<OUT> out) {
        out.put((OUT)in);
    }

    public void close(Pipe<OUT> out) {
        // no op by default
    	finish();
    }

	@Override
	public void finish() {
		// TODO Auto-generated method stub
	}

}
