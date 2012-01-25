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
 * Copyright 2012 Think Big Analytics. All Rights Reserved.
 */
package tap.core;

/**
 * @author Douglas Moore
 *
 */
public interface TapInterface {

	/**
	 * Factory to create Phase and link it back to the Tap.
	 * @return
	 */
	public Phase createPhase();
	   
	/**
	 * 'Make' all dependencies, submit phases etc.
	 * @return
	 */
	public int make();
	
	/**
	 * Subscribe interface
	 * @param URI e.g. "//cta/candle/1min[AAPL]"
	 * @return Pipe subscribed to URI
	 */
	public Pipe subscribe(String URI);

}
