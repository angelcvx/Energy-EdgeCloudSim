/*
 * Title:        EdgeCloudSim - Basic Edge Orchestrator implementation
 * 
 * Description: 
 * BasicEdgeOrchestrator implements basic algorithms which are
 * first/next/best/worst/random fit algorithms while assigning
 * requests to the edge devices.
 *               
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 * Copyright (c) 2017, Bogazici University, Istanbul, Turkey
 */

package edu.boun.edgecloudsim.edge_orchestrator;

import java.util.List;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;

import edu.boun.edgecloudsim.cloud_server.CloudVM;
import edu.boun.edgecloudsim.core.SimManager;
import edu.boun.edgecloudsim.core.SimSettings;
import edu.boun.edgecloudsim.edge_server.EdgeVM;
import edu.boun.edgecloudsim.edge_client.CpuUtilizationModel_Custom;
import edu.boun.edgecloudsim.edge_client.Task;
import edu.boun.edgecloudsim.utils.Location;
import edu.boun.edgecloudsim.utils.SimUtils;
import edu.boun.edgecloudsim.utils.SimLogger;
import java.util.stream.DoubleStream;
import org.cloudbus.cloudsim.Datacenter;

public class BasicEdgeOrchestrator extends EdgeOrchestrator {
	private int numberOfHost; //used by load balancer
	private int lastSelectedHostIndex; //used by load balancer
	private int[] lastSelectedVmIndexes; //used by each host individually
	
        public BasicEdgeOrchestrator(String _policy, String _simScenario) {
		super(_policy, _simScenario);
	}

	@Override
	public void initialize() {
                //angel:modifying number of hosts used by the orchestrator
		numberOfHost=SimSettings.getInstance().getNumOfEdgeHosts();
		lastSelectedHostIndex = -1;
		lastSelectedVmIndexes = new int[numberOfHost];
		for(int i=0; i<numberOfHost; i++)
			lastSelectedVmIndexes[i] = -1;
	}

	@Override
	public int getDeviceToOffload(Task task) {
		int result = SimSettings.GENERIC_EDGE_DEVICE_ID;
		if(!simScenario.equals("SINGLE_TIER")){
			//decide to use cloud or Edge VM
			int CloudVmPicker = SimUtils.getRandomNumber(0, 100);
			
			if(CloudVmPicker <= SimSettings.getInstance().getTaskLookUpTable()[task.getTaskType()][1])
				result = SimSettings.CLOUD_DATACENTER_ID;
			else
				result = SimSettings.GENERIC_EDGE_DEVICE_ID;
		}
		
		return result;
	}
	
	@Override
	public Vm getVmToOffload(Task task, int deviceId) {
		Vm selectedVM = null;
		
		if(deviceId == SimSettings.CLOUD_DATACENTER_ID){
			//Select VM on cloud devices via Least Loaded algorithm!
			double selectedVmCapacity = 0; //start with min value
			List<Host> list = SimManager.getInstance().getCloudServerManager().getDatacenter().getHostList();
			for (int hostIndex=0; hostIndex < list.size(); hostIndex++) {
				List<CloudVM> vmArray = SimManager.getInstance().getCloudServerManager().getVmList(hostIndex);
				for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
					double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if(requiredCapacity <= targetVmCapacity && targetVmCapacity > selectedVmCapacity){
						selectedVM = vmArray.get(vmIndex);
						selectedVmCapacity = targetVmCapacity;
					}
                                }
			}
		}
		else if(simScenario.equals("TWO_TIER_WITH_EO"))
			selectedVM = selectVmOnLoadBalancer(task);
		else
			selectedVM = selectVmOnHost(task);
		
		return selectedVM;
	}
	
        //this method selects the host and VM appropiated for each task
	public EdgeVM selectVmOnHost(Task task){
		EdgeVM selectedVM = null;
		double energyConsumptionAssignment = 999999999;
                double idleEnergyConsumption = 0;
		Location deviceLocation = SimManager.getInstance().getMobilityModel().getLocation(task.getMobileDeviceId(), CloudSim.clock());
                //in our scenasrio, serving wlan ID is equal to the host id
		//because there is only one host in one place
                
		int relatedHostId=deviceLocation.getServingWlanId();
     		List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(relatedHostId);
		
		if(policy.equalsIgnoreCase("RANDOM_FIT")){
			int randomIndex = SimUtils.getRandomNumber(0, vmArray.size()-1);
			double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(randomIndex).getVmType());
			double targetVmCapacity = (double)100 - vmArray.get(randomIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
			if(requiredCapacity <= targetVmCapacity)
				selectedVM = vmArray.get(randomIndex);

		}
		else if(policy.equalsIgnoreCase("WORST_FIT")){
			double selectedVmCapacity = 0; //start with min value
			for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
				double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
				double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
				if(requiredCapacity <= targetVmCapacity && targetVmCapacity > selectedVmCapacity){
					selectedVM = vmArray.get(vmIndex);
					selectedVmCapacity = targetVmCapacity;
				}
			}

		}
		else if(policy.equalsIgnoreCase("BEST_FIT")){
			double selectedVmCapacity = 101; //start with max value
			for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
				double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
				double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
				if(requiredCapacity <= targetVmCapacity && targetVmCapacity < selectedVmCapacity){
					selectedVM = vmArray.get(vmIndex);
					selectedVmCapacity = targetVmCapacity;
				}
			}

		}
		else if(policy.equalsIgnoreCase("FIRST_FIT")){
			for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
				double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
				double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
				if(requiredCapacity <= targetVmCapacity){
					selectedVM = vmArray.get(vmIndex);
					break;
				}
			}

		}
		else if(policy.equalsIgnoreCase("NEXT_FIT")){
			int tries = 0;
			while(tries < vmArray.size()){
				lastSelectedVmIndexes[relatedHostId] = (lastSelectedVmIndexes[relatedHostId]+1) % vmArray.size();
				double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(lastSelectedVmIndexes[relatedHostId]).getVmType());
				double targetVmCapacity = (double)100 - vmArray.get(lastSelectedVmIndexes[relatedHostId]).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
				if(requiredCapacity <= targetVmCapacity){
					selectedVM = vmArray.get(lastSelectedVmIndexes[relatedHostId]);
					break;
				}
				tries++;
			}
		}
                else if(policy.equalsIgnoreCase("MIMNIMIZE_ENERGY_CONSUMPTION")){
			double selectedVmCapacity = 101; //start with max value
                        for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
                                double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
                                double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());				
                                //calculating energy consumption
                                double energyConsumption [] = calculateEnergyConsumption(vmArray.get(vmIndex), task);
                                double wholeEnergyConsumption = DoubleStream.of(calculateEnergyConsumption(vmArray.get(vmIndex), task)).sum();                                                 
                                if (wholeEnergyConsumption < energyConsumptionAssignment){
                                        if(requiredCapacity <= targetVmCapacity && targetVmCapacity < selectedVmCapacity){
                                                selectedVM = vmArray.get(vmIndex);
                                                selectedVmCapacity = targetVmCapacity;
                                                energyConsumptionAssignment = wholeEnergyConsumption;
                                                idleEnergyConsumption = energyConsumption[0];
                                        }
                                }
                        }	
		}
		return selectedVM;
	}

	public EdgeVM selectVmOnLoadBalancer(Task task){
		EdgeVM selectedVM = null;
		SimLogger SS = SimLogger.getInstance();
                double energyConsumptionAssignment = 999999999;
                double idleEnergyConsumption = 0;
                
		if(policy.equalsIgnoreCase("RANDOM_FIT")){
			int randomHostIndex = SimUtils.getRandomNumber(0, numberOfHost-1);
			List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(randomHostIndex);
			int randomIndex = SimUtils.getRandomNumber(0, vmArray.size()-1);
			
			double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(randomIndex).getVmType());
			double targetVmCapacity = (double)100 - vmArray.get(randomIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
			if(requiredCapacity <= targetVmCapacity)
				selectedVM = vmArray.get(randomIndex);

                }
		else if(policy.equalsIgnoreCase("WORST_FIT")){
			double selectedVmCapacity = 0; //start with min value
			for(int hostIndex=0; hostIndex<numberOfHost; hostIndex++){
				List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(hostIndex);
				for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
					double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if(requiredCapacity <= targetVmCapacity && targetVmCapacity > selectedVmCapacity){
						selectedVM = vmArray.get(vmIndex);
						selectedVmCapacity = targetVmCapacity;
					}
				}
			}
		}
		else if(policy.equalsIgnoreCase("BEST_FIT")){
			double selectedVmCapacity = 101; //start with max value
			for(int hostIndex=0; hostIndex<numberOfHost; hostIndex++){
				List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(hostIndex);
				for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
					double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if(requiredCapacity <= targetVmCapacity && targetVmCapacity < selectedVmCapacity){
						selectedVM = vmArray.get(vmIndex);
						selectedVmCapacity = targetVmCapacity;
					}
				}
			}
		}
		else if(policy.equalsIgnoreCase("FIRST_FIT")){
			for(int hostIndex=0; hostIndex<numberOfHost; hostIndex++){
				List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(hostIndex);
				for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
					double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if(requiredCapacity <= targetVmCapacity){
						selectedVM = vmArray.get(vmIndex);
                                                break;
					}
				}
			}
		}
		else if(policy.equalsIgnoreCase("NEXT_FIT")){
			int hostCheckCounter = 0;	
			while(selectedVM == null && hostCheckCounter < numberOfHost){
				int tries = 0;
				lastSelectedHostIndex = (lastSelectedHostIndex+1) % numberOfHost;
				List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(lastSelectedHostIndex);
				while(tries < vmArray.size()){
					lastSelectedVmIndexes[lastSelectedHostIndex] = (lastSelectedVmIndexes[lastSelectedHostIndex]+1) % vmArray.size();
					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(lastSelectedVmIndexes[lastSelectedHostIndex]).getVmType());
					double targetVmCapacity = (double)100 - vmArray.get(lastSelectedVmIndexes[lastSelectedHostIndex]).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if(requiredCapacity <= targetVmCapacity){
						selectedVM = vmArray.get(lastSelectedVmIndexes[lastSelectedHostIndex]);
                                                break;
					}
					tries++;
				}

				hostCheckCounter++;
			}
		} 
                else if(policy.equalsIgnoreCase("MINIMIZE_ENERGY_CONSUMPTION")){
			for(int hostIndex=0; hostIndex<numberOfHost; hostIndex++){
				List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(hostIndex);
				for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
					double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
                                        //calculating energy consumption
                                        double wholeEnergyConsumption = DoubleStream.of(calculateEnergyConsumption(vmArray.get(vmIndex), task)).sum();
                                        if (wholeEnergyConsumption < energyConsumptionAssignment && requiredCapacity <= targetVmCapacity){            
                                                selectedVM = vmArray.get(vmIndex);
                                                energyConsumptionAssignment = wholeEnergyConsumption;
					} 
				}
			}                        
		}
                //if the host is not active yet, we set its status and starttime (without warm up delay)
                if (selectedVM.getHost().isActive() == false) {
                    selectedVM.getHost().setStatus(true);
                    selectedVM.getHost().setStartedTime(CloudSim.clock());
                }
                return selectedVM;
	}

	@Override
	public void processEvent(SimEvent arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void shutdownEntity() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startEntity() {
		// TODO Auto-generated method stub
		
	}
        
        public void deactivateHost (Host host) {
            double idleEnergyConsumption = calculateIdleEnergyConsumption (host, CloudSim.clock() - host.getStartedTime());
            host.setStatus(false);
            host.setStartedTime(0);
            SimLogger.getInstance().incrementEnergyConsumption(idleEnergyConsumption, host.getId());
        }
        
        public void activeHost (Host host) {
            host.setStatus(true);
            host.setStartedTime(CloudSim.clock());
        }
        
        public double  calculateIdleEnergyConsumption (Host host, double time) {
            double result = 0;
            result = host.getIdleEnergyConsumption() * host.getmaxEnergyConsumption() * time;
            return result;
        }
        
        //this function return two variables, the idle and the dynamic energy consumption
        public double [] calculateEnergyConsumption(EdgeVM vm, Task task) {
                //the first 
                double idleEnergyConsumption = 0;
                double dynamicEnergyConsumption;
                //obtaining variables
                double dataToRecive = task.getCloudletFileSize(); //unid?
                double dataToSend = task.getCloudletOutputSize(); //unid?
                Host host = vm.getHost();
                double taskRequiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vm.getVmType());
                //remaining time of the simulation from now to the end
                double simulationRemainingTime = SimSettings.getInstance().getSimulationTime() - CloudSim.clock();
                //relation between the total amount of mips and the vm's mips
                double relationCPU = host.getTotalMips()/vm.getMips();
                //it the host was idle before the execution, we add the base energy consumption to the result 
                if (host.isActive() == false){
                        idleEnergyConsumption = host.getmaxEnergyConsumption() * host.getIdleEnergyConsumption() * simulationRemainingTime;
                }
                //calculating dynamic energy consumption
                    //(1) computation's energy consumption
                //energy = (idleEnergy*maxEnergy + (1-idleEnergy)*maxEnergy*percetageCPUApp)*energyWeight                
                dynamicEnergyConsumption = ((1 - host.getIdleEnergyConsumption()) * host.getmaxEnergyConsumption() * (taskRequiredCapacity/100)) * relationCPU * host.getEnergyWeight() * task.getCloudletTotalLength()/vm.getMips();
                    //(2) consumption due to data transmission
                dynamicEnergyConsumption += dataToRecive/vm.getBw() * host.getTransmissionPower() + dataToSend/vm.getBw() * host.getReceptionPower();
                return new double [] {idleEnergyConsumption, dynamicEnergyConsumption};
        }
        
        //power consumption without considering the exeuction time (W)
        public double calculatePowerConsumption(EdgeVM vm, double vmCPUrequired) {
                //obtaining variables
                double energyConsumption = 0;
                Host host = vm.getHost();
                double maxEnergyConsumptionHost = host.getmaxEnergyConsumption();
                double idleEnergyConsumptionHost = host.getIdleEnergyConsumption();
                double energyWeightHost = host.getEnergyWeight();

                //relation between the total amount of mips and the vm's mips
                double relationCPU = host.getTotalMips()/vm.getMips();
                //it the host was idle before the execution, we add the base energy consumption to the result 
                if (host.getAvailableMips() == host.getTotalMips()){
                        energyConsumption += maxEnergyConsumptionHost * idleEnergyConsumptionHost;
                }
                //energy = (idleEnergy*maxEnergy + (1-idleEnergy)*maxEnergy*percetageCPUApp)*energyWeight
                energyConsumption += ((1 - idleEnergyConsumptionHost) * maxEnergyConsumptionHost * (vmCPUrequired/100)) * relationCPU * energyWeightHost;
                return energyConsumption;
        }
        
}