package org.apache.drill.exec.physical.impl.project;

public class OutputWidthVisitorState {

    ProjectMemoryManager manager;
    ProjectMemoryManager.OutputColumnType outputColumnType;

    public OutputWidthVisitorState(ProjectMemoryManager manager, ProjectMemoryManager.OutputColumnType outputColumnType) {
        this.manager = manager;
        this.outputColumnType = outputColumnType;
    }

    public ProjectMemoryManager getManager() {
        return manager;
    }

    public ProjectMemoryManager.OutputColumnType getOutputColumnType() {
        return outputColumnType;
    }
}
