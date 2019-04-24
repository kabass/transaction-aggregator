package com.carrefour.phenix.util

import java.time.LocalDate

import com.beust.jcommander.Parameter
import com.carrefour.phenix.service.impl.DATE_FORMATTER

object CommandLineArgs {
  @Parameter(names = Array("-h", "--help"), help = true)
  var help = false

  @Parameter(names = Array("-i", "--input"), description = "the input directory", required = true)
  var inputDirectory: String = _

  @Parameter(names = Array("-a", "--archive"), description = "the archive directory", required = false)
  var archiveDirectory: String = _

  @Parameter(names = Array("-o", "--output"), description = "the ouput directory", required = true)
  var outputDirectory: String = _

  @Parameter(names = Array("-d", "--date"), description = "the date to process : default current day", required = false)
  var transactionDateString = LocalDate.now.format(DATE_FORMATTER)

  @Parameter(names = Array("-t", "--top"), description = "the the number of top elements : default 100", required = false)
  var topLimit = 100

  @Parameter(names = Array("-p", "--period"), description = "the period lenght : default 7", required = false)
  var periodLength = 7

  @Parameter(names = Array("-pa", "--partionNumber"), description = "the data partition number : default 8", required = false)
  var partitionNumber = 8

}
