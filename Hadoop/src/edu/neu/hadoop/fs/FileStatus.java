package edu.neu.hadoop.fs;

/**
 * Util class for checking file status
 * 
 * @author Adib Alwani
 */
public class FileStatus {
	
	private Path path;
	
	public FileStatus(Path path) {
		this.path = path;
	}
	
	public Path getPath() {
		return path;
	}
}
