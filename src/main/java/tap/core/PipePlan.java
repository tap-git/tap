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

import java.util.*;

class PipePlan {

    private Map<Pipe, Phase> fileDeps = new LinkedHashMap<Pipe, Phase>();
    private Map<Phase, Set<Pipe>> processDeps = new LinkedHashMap<Phase, Set<Pipe>>();
    private Set<Pipe> prebuilt = new HashSet<Pipe>();
    private Set<Phase> failed = new HashSet<Phase>();
    private List<List<Phase>> waves = new ArrayList<List<Phase>>();
    private Set<Phase> executing = new HashSet<Phase>();

    public synchronized void fileCreateWith(Pipe file, Phase process) {
        if (process == null) {
            prebuilt.add(file);
        } else {
            fileDeps.put(file, process);
        }
    }

    public synchronized void processReads(Phase process, Pipe file) {
        Set<Pipe> procDep = processDeps.get(process);
        if (procDep == null) {
            procDep = new HashSet<Pipe>();
            processDeps.put(process, procDep);
        }
        procDep.add(file);
    }
    
    public synchronized void failed(Phase process) {
        failed.add(process);
        executing.remove(process);
    }
    
    public synchronized void updated(Phase process) {
        executing.remove(process);
        // remove dependence ON this process from files that it generates - fileDeps
        for (Pipe file : process.getOutputs()) {
            prebuilt.add(file);
            fileDeps.remove(file);
        }
        processDeps.remove(process);
        // could incrementally update plan, but for now we just do it in batch
    }
    
    public synchronized List<List<Phase>> plan() {
        Set<Pipe> toPlan = getFileDependencies();
        waves.clear();
        if (!failed.isEmpty())
            return null; // don't run anything else in a failed job

        // pull out all the leaf nodes (i.e., those with no unplanned dependencies) as another parallel wave that can execute
        // this doesn't specify exact scheduling as processes finish but provides more parallelism than pure serial operation 
        while (!toPlan.isEmpty()) {
            Set<Pipe> wave = new HashSet<Pipe>();
            Iterator<Pipe> it = toPlan.iterator();
            while (it.hasNext()) {
                Pipe file = it.next();
                Phase process = fileDeps.get(file);
                if (!executing.contains(process)) {
                    boolean canRemove = true;
                    for (Pipe dependency : processDeps.get(process)) {
                        if (toPlan.contains(dependency)) {
                            canRemove = false;
                            break;
                        }
                    }
                    if (canRemove) {
                        wave.add(file);
                    }
                } else {
                    it.remove(); // already being built...
                }
            }
            if (wave.isEmpty() && executing.isEmpty()) {
                StringBuilder cycle = new StringBuilder();
                for (Pipe file : toPlan) {
                    cycle.append(' ').append(file.getPath());
                }
                throw new IllegalStateException("Cyclic dependency among files: "+cycle);
            }
            List<Phase> nextWave = new ArrayList<Phase>();
            for (Pipe file : wave) {
                toPlan.remove(file);
            
                // we should really score these and order them by some kind of priority
                // for now we submit everything that can be done in parallel
                if (!nextWave.contains(file.getProducer()))
                    nextWave.add(file.getProducer());
            }            
            waves.add(nextWave);
        }
        return waves;
    }
    
    /**
     * Must call AFTER calling plan.
     */
    public synchronized List<Phase> getNextProcesses() {
        return (waves==null || waves.isEmpty()) ? null : Collections.unmodifiableList(waves.get(0));
    }

    public synchronized boolean executing(Phase process) {
        return executing.add(process);        
    }

    public synchronized boolean isComplete() {
        return executing.isEmpty() && (!failed.isEmpty() || fileDeps.isEmpty());
    }

    public Set<Pipe> getFileDependencies() {
        return new HashSet<Pipe>(fileDeps.keySet());
    }
}
