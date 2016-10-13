package calcite.planner.physical;

import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.SystemConf.SchedulingPolicy;

public class SystemConfig {
	SystemConf systemConf;
	
	/** Default system configuration. */
	public SystemConfig() {
		this.systemConf = new SystemConf();
	}
	
	/** Set system configuration. */
	public SystemConfig(SystemConf systemConf) {
		this.systemConf = new SystemConfig().setConfig(systemConf).build();
	}
	
	public SystemConfig setConfig (SystemConf systemConf){
		this.systemConf = systemConf;
		return this;
	}
	
	/* Comments have to be added.*/ 
	public SystemConfig setWWW (boolean isWWW) {
		this.systemConf.WWW = isWWW;
		return this;
	} 
	  
	public SystemConfig setSchedulingPolicy (SchedulingPolicy schedulingPolicy) {
	    this.systemConf.SCHEDULING_POLICY = schedulingPolicy ;
	    return this;
	}
	  
	public SystemConfig setSwitchThreshold (int switchThreshold) {
	    this.systemConf.SWITCH_THRESHOLD = switchThreshold;
	    return this;
	}
	  
	public SystemConfig setPartialWindows (int partialWindows) {
	    this.systemConf.PARTIAL_WINDOWS = partialWindows;
	    return this;
	}
	  
	public SystemConfig setHashTableSize (int hashTableSize) {
	    this.systemConf.HASH_TABLE_SIZE = hashTableSize;
	    return this;
	}
	  
	public SystemConfig setThroughputMonitorInterval (long throughputMonitorInterval) {
	    this.systemConf.THROUGHPUT_MONITOR_INTERVAL = throughputMonitorInterval;
	    return this;
	}
	  
	public SystemConfig setPerfomanceMonitorInterval (long perfomanceMonitorInterval) {
	    this.systemConf.PERFORMANCE_MONITOR_INTERVAL = perfomanceMonitorInterval;
	    return this;
	}
	  
	public SystemConfig setMostUpstreamQueries (int mostUpstreamQueries) {
	    this.systemConf.MOST_UPSTREAM_QUERIES = mostUpstreamQueries;
	    return this;
	}
	  
	public SystemConfig setPipelineDepth (int pipelineDepth) {
	    this.systemConf.PIPELINE_DEPTH = pipelineDepth;
	    return this;
	}
	  
	public SystemConfig setCircularBufferSize (int circularBufferSize) {
	    this.systemConf.CIRCULAR_BUFFER_SIZE = circularBufferSize;
	    return this;
	}
	  
	public SystemConfig setUnboundedBufferSize (int unboundedBufferSize) {
	    this.systemConf.UNBOUNDED_BUFFER_SIZE = unboundedBufferSize;
	    return this;
	}
	  
	public SystemConfig setThreads (int threads) {
	    this.systemConf.THREADS = threads;
	    return this;
	}
	  
	public SystemConfig setSlots (int slots) {
	    this.systemConf.SLOTS = slots;
	    return this;
	}
	  
	public SystemConfig setCPU (boolean isCPU) {
	    this.systemConf.CPU = isCPU;
	    return this;
	}
	  
	public SystemConfig setGPU (boolean isGPU) {
	    this.systemConf.GPU = isGPU;
	    return this;
	}
	  
	public SystemConfig setHybrid (boolean isHybrid) {
	    this.systemConf.HYBRID = isHybrid;
	    return this;
	}
	  
	public SystemConfig setLatencyOn (boolean isLatencyOn) {
	   this.systemConf.LATENCY_ON = isLatencyOn;
	   return this;
	}
	
	public SystemConf build(){
		return this.systemConf;
	}
	
}
