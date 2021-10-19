package com.risingwave.gradle.plugin.git
import java.io.ByteArrayOutputStream
import java.util.regex.Pattern
import org.gradle.api.Plugin
import org.gradle.api.Project
import java.io.File

fun Project.getExecOutput(command: String): List<String> {
  val output = ByteArrayOutputStream()
  this.exec {
    commandLine = command.split(" ")
    standardOutput = output
    errorOutput = System.err
  }

  val result = output.toString().trim().split("\n")
  output.close()
  return result
}

fun Project.getCurrentBranch(): String {
  return this.getExecOutput("git branch --show-current")[0]
}

fun Project.getMergeBase(target: String, source: String): String {
  return this.getExecOutput("git merge-base $target $source")[0]
}

/**
 * Get all files that are changed but not deleted nor renamed.
 * Compares to master or the specified target branch.
 *
 * @return List of all changed files
 */
fun Project.getChangedFiles(): List<String> {
  // Get the target and source branch
  val ghprbTargetBranch = System.getenv("GITHUB_BASE_REF")
  val ghprbSourceBranch = System.getenv("GITHUB_HEAD_REF")

  println("ghprbTargetBranch: $ghprbTargetBranch, ghprbSourceBranch: $ghprbSourceBranch")

  // Compare to master if no branch specified
  val targetBranch = ghprbTargetBranch?.let { "origin/$it" } ?: "origin/master"
  val sourceBranch = ghprbSourceBranch?.let { "origin/$it" } ?: this.getCurrentBranch()

  println("targetBranch: $targetBranch, sourceBranch: $sourceBranch")

  val mergeBase = this.getMergeBase(targetBranch, sourceBranch)
  println("merge base: $mergeBase")

  // Get list of all changed files including status
  val systemOutStream = ByteArrayOutputStream();
  this.exec {
    commandLine = "git diff --name-status --diff-filter=dr $mergeBase $sourceBranch".split(" ");
    standardOutput = systemOutStream
    errorOutput = System.err
  }

  val allFiles = systemOutStream.toString().trim().split('\n')
  systemOutStream.close()

  // Remove the status prefix
  val statusPattern = Pattern.compile("(\\w)\\t+(.+)")
  val files = mutableListOf<String>()
  for (file in allFiles) {
    val matcher = statusPattern.matcher(file)
    if (matcher.find()) {
      val s = matcher.group(2)
      files.add(File(rootDir.parent, s).absolutePath)
    }
  }

  // Return the list of touched files
  return files
}

open class GitPlugin : Plugin<Project> {
  override fun apply(project: Project) {
    project.getExtensions().getExtraProperties().set("changedFiles", project.getChangedFiles())
  }
}
