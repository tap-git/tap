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

import java.net.URI;
import java.util.*;
import java.util.concurrent.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;

import tap.util.Alerter;
import tap.util.EmailAlerter;

@SuppressWarnings("deprecation")
public class Tap {
    private List<Pipe> writes;
    private int parallelPhases = 2; // default to 2 phases at once - use
                                    // concurrency (also speeds up local
                                    // running)
    private JobConf baseConf = new JobConf();
    private Alerter alerter = new EmailAlerter();
    private String name = "";
    private boolean forceRebuild = false;

    public Tap() {
    }

    public Tap(Class<?> jarClass) {
        baseConf.setJarByClass(jarClass);
        baseConf.set("mapred.job.reuse.jvm.num.tasks", "-1");
        try {
            FileSystem fs = FileSystem.get(new URI("/"), baseConf);
            FileSystem localfs = FileSystem.getLocal(baseConf);
            if (fs.equals(localfs)) {
                baseConf.setNumReduceTasks(2); // run only 2 reducers for local
            } else {
                baseConf.setNumReduceTasks(32); // default to 32 reducers - need
                                                // to tune this
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Tap produces(List<Pipe> outputs) {
        if (writes == null) {
            writes = new ArrayList<Pipe>(outputs);
        } else {
            writes.addAll(outputs);
        }
        return this;
    }

    public Tap produces(Pipe... outputs) {
        return produces(Arrays.asList(outputs));
    }

    public Tap named(String name) {
        this.name = name;
        return this;
    }

    public Tap forceRebuild() {
        this.forceRebuild = true;
        return this;
    }

    public Tap parallelPhases(int parallelPhases) {
        this.parallelPhases = parallelPhases;
        return this;
    }

    public int getParallelPhases() {
        return parallelPhases;
    }

    public void execute() throws InfeasiblePlanException {
        List<PhaseError> result = new ArrayList<PhaseError>();
        PipePlan plan = generatePlan(result);
        if (result.isEmpty()) {
            plan = optimize(plan, result);
            if (result.isEmpty()) {
                execute(plan, result);
            }
        }
        respond(result);
    }

    private void respond(List<PhaseError> result) {
        if (!result.isEmpty()) {
            alerter.alert(result);
        } else {
            alerter.pipeCompletion(getName(),
                    "Execution completed successfully.");
        }
    }

    private List<PhaseError> execute(final PipePlan plan,
            List<PhaseError> errors) {
        ExecutorService execution = Executors
                .newFixedThreadPool(parallelPhases);
        submit(plan, execution, Collections.synchronizedList(errors));

        try {
            synchronized (plan) {
                while (!plan.isComplete() && errors.isEmpty()) {
                    plan.wait();
                }
            }
            execution.shutdown();
            execution.awaitTermination(20L, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            System.err.println("interrupted job");
            alerter.alert("pipe execution interrupted");
        }
        return errors;
    }

    int submission = 0;

    private List<PhaseError> submit(final PipePlan plan,
            final ExecutorService execution, final List<PhaseError> errors) {
        List<Phase> next = plan.getNextProcesses();
        if (next != null) {
            for (final Phase process : next) {
                if (plan.executing(process)) {
                    execution.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                PhaseError error = process.submit();
                                if (error != null) {
                                    errors.add(error);
                                    // alert immediately
                                    System.err.println("Phase failed: "
                                            + error.getMessage());
                                    plan.failed(process);
                                } else {
                                    plan.updated(process);
                                    plan.plan();
                                    submit(plan, execution, Collections
                                            .synchronizedList(errors));
                                }
                            } finally {
                                synchronized (plan) {
                                    plan.notify();
                                }
                            }
                        }

                    });
                }
            }
        }
        return errors;
    }

    private PipePlan optimize(PipePlan plan, List<PhaseError> result) {
        // not yet
        return plan;
    }

    private PipePlan generatePlan(List<PhaseError> errors) {
        Set<Pipe> toGenerate = new HashSet<Pipe>(writes);
        Set<Pipe> generated = new HashSet<Pipe>();
        Set<Phase> planned = new HashSet<Phase>();
        Set<Pipe> obsolete = new HashSet<Pipe>();
        Set<Pipe> missing = new HashSet<Pipe>();
        // Map<DistFile,Collection<DistFile>> dependencies;
        PipePlan plan = new PipePlan();
        while (!toGenerate.isEmpty()) {
            Pipe file = toGenerate.iterator().next();
            toGenerate.remove(file);
//            boolean exists = file.exists(baseConf);
            boolean exists = true;
//            if (exists && !file.isObsolete(baseConf)
//                    && (!forceRebuild || file.getProducer() == null)) {
            if (file.getProducer() == null) {
                if (!generated.contains(file)) {
                    System.out.println("File: " + file.getPath()
                            + " exists and is up to date.");
                    // ok already
                    generated.add(file);
                    plan.fileCreateWith(file, null);
                }
            } else {
                Phase phase = file.getProducer();
                plan.fileCreateWith(file, phase);
                if (phase == null) {
                    errors.add(new PhaseError("Don't know how to generate "
                            + file.getPath()));
                } else {
                    if (!exists)
                        missing.add(file);
                    else if (file.isObsolete(baseConf))
                        obsolete.add(file);
                    List<Pipe> inputs = phase.getInputs();
                    if (inputs != null) {
                        for (Pipe input : inputs) {
                            toGenerate.add(input);
                            plan.processReads(phase, input);
                        }
                    }
                    if (!planned.contains(phase)) {
                        List<PhaseError> phaseErrors = phase.plan(this);
                        if (!phaseErrors.isEmpty()) {
                            errors.addAll(phaseErrors);
                            return null;
                        }
                        planned.add(phase);
                    }
                }
            }
        }
        List<List<Phase>> waves = plan.plan();

        // partially ordered so we always print a producer before a consumer
        for (List<Phase> wave : waves) {
            for (Phase phase : wave) {
                System.out.println("Will run " + phase.getSummary()
                        + ", producing: ");
                for (Pipe output : phase.getOutputs()) {
                    System.out.print("  " + output.getPath());
                    if (missing.contains(output))
                        System.out.println(": missing");
                    else if (obsolete.contains(output))
                        System.out.println(": obsolete");
                    else
                        System.out.println();
                }
            }
        }
        return plan;
    }

    public String getName() {
        return name;
    }

    public JobConf getConf() {
        return baseConf;
    }

    public void dryRun() throws InfeasiblePlanException {
        List<PhaseError> result = new ArrayList<PhaseError>();
        PipePlan plan = generatePlan(result);
        if (result.isEmpty()) {
            plan = optimize(plan, result);
        }
        if (!result.isEmpty()) {
            System.out.println("Plan errors:");
            for (PhaseError e : result) {
                System.out.println(e.getMessage());
            }
        }
    }

}
