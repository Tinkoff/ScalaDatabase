/*
 * Copyright (c) 2013.
 * Tinkoff Credit Systems.
 * All rights reserved.
 */

package ru.tcsbank

/**
 * Bits of handy stuff
 */
package object utils {

  // Simple class for universal type-safe notion of id
  case class Id[+T](value: String)

  // Simple time representation
  case class Time(milliseconds: Long)

  // Utility to convert imperative (by side-effects) object instance initialisation
  // to a more functional: making initialisation as a single expression returning a value
  def make[T](t: T)(f: T => _): T = { f(t); t }

  // Simple resource management utility
  def using[A, B <: { def close(): Unit }](closeable: B) (f: B => A): A =
    try { f(closeable) } finally { closeable.close() }
}