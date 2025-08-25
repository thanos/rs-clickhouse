//! Geometric data types for ClickHouse

use super::Value;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Point type (2D coordinate)
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Point(pub f64, pub f64);

/// Ring type (closed line string)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ring(pub Vec<Point>);

/// Polygon type (area bounded by rings)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Polygon(pub Vec<Ring>);

/// MultiPolygon type (collection of polygons)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MultiPolygon(pub Vec<Polygon>);

impl Point {
    /// Create a new point with x and y coordinates
    pub fn new(x: f64, y: f64) -> Self {
        Point(x, y)
    }

    /// Get the x coordinate
    pub fn x(&self) -> f64 {
        self.0
    }

    /// Get the y coordinate
    pub fn y(&self) -> f64 {
        self.1
    }

    /// Set the x coordinate
    pub fn set_x(&mut self, x: f64) {
        self.0 = x;
    }

    /// Set the y coordinate
    pub fn set_y(&mut self, y: f64) {
        self.1 = y;
    }

    /// Calculate the distance to another point
    pub fn distance_to(&self, other: &Point) -> f64 {
        let dx = self.0 - other.0;
        let dy = self.1 - other.1;
        (dx * dx + dy * dy).sqrt()
    }

    /// Calculate the squared distance to another point (faster than distance_to)
    pub fn distance_squared_to(&self, other: &Point) -> f64 {
        let dx = self.0 - other.0;
        let dy = self.1 - other.1;
        dx * dx + dy * dy
    }

    /// Check if the point is finite (not NaN or infinite)
    pub fn is_finite(&self) -> bool {
        self.0.is_finite() && self.1.is_finite()
    }

    /// Check if the point is valid (finite coordinates)
    pub fn is_valid(&self) -> bool {
        self.is_finite()
    }
}

impl Ring {
    /// Create a new empty ring
    pub fn new() -> Self {
        Ring(Vec::new())
    }

    /// Create a new ring from points
    pub fn from_points(points: Vec<Point>) -> Self {
        Ring(points)
    }

    /// Create a new ring with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Ring(Vec::with_capacity(capacity))
    }

    /// Get the number of points in the ring
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the ring is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get a point by index
    pub fn get(&self, index: usize) -> Option<&Point> {
        self.0.get(index)
    }

    /// Get a mutable point by index
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Point> {
        self.0.get_mut(index)
    }

    /// Add a point to the ring
    pub fn push(&mut self, point: Point) {
        self.0.push(point);
    }

    /// Remove the last point from the ring
    pub fn pop(&mut self) -> Option<Point> {
        self.0.pop()
    }

    /// Insert a point at the specified index
    pub fn insert(&mut self, index: usize, point: Point) {
        self.0.insert(index, point);
    }

    /// Remove a point at the specified index
    pub fn remove(&mut self, index: usize) -> Point {
        self.0.remove(index)
    }

    /// Clear all points from the ring
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Check if the ring is closed (first and last points are the same)
    pub fn is_closed(&self) -> bool {
        if self.0.len() < 3 {
            return false;
        }
        self.0.first() == self.0.last()
    }

    /// Close the ring by adding the first point at the end if not already closed
    pub fn close(&mut self) {
        if !self.is_closed() && !self.0.is_empty() {
            if let Some(first) = self.0.first() {
                self.0.push(*first);
            }
        }
    }

    /// Calculate the perimeter of the ring
    pub fn perimeter(&self) -> f64 {
        if self.0.len() < 2 {
            return 0.0;
        }

        let mut perimeter = 0.0;
        for i in 0..self.0.len() - 1 {
            perimeter += self.0[i].distance_to(&self.0[i + 1]);
        }
        perimeter
    }

    /// Calculate the area of the ring (using shoelace formula)
    pub fn area(&self) -> f64 {
        if self.0.len() < 3 {
            return 0.0;
        }

        let mut area = 0.0;
        for i in 0..self.0.len() {
            let j = (i + 1) % self.0.len();
            area += self.0[i].x() * self.0[j].y();
            area -= self.0[j].x() * self.0[i].y();
        }
        area.abs() / 2.0
    }

    /// Check if the ring is valid (closed and has at least 3 points)
    pub fn is_valid(&self) -> bool {
        self.len() >= 3 && self.is_closed()
    }
}

impl Polygon {
    /// Create a new empty polygon
    pub fn new() -> Self {
        Polygon(Vec::new())
    }

    /// Create a new polygon from rings
    pub fn from_rings(rings: Vec<Ring>) -> Self {
        Polygon(rings)
    }

    /// Create a new polygon with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Polygon(Vec::with_capacity(capacity))
    }

    /// Get the number of rings in the polygon
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the polygon is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get a ring by index
    pub fn get(&self, index: usize) -> Option<&Ring> {
        self.0.get(index)
    }

    /// Get a mutable ring by index
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Ring> {
        self.0.get_mut(index)
    }

    /// Add a ring to the polygon
    pub fn push(&mut self, ring: Ring) {
        self.0.push(ring);
    }

    /// Remove the last ring from the polygon
    pub fn pop(&mut self) -> Option<Ring> {
        self.0.pop()
    }

    /// Insert a ring at the specified index
    pub fn insert(&mut self, index: usize, ring: Ring) {
        self.0.insert(index, ring);
    }

    /// Remove a ring at the specified index
    pub fn remove(&mut self, index: usize) -> Ring {
        self.0.remove(index)
    }

    /// Clear all rings from the polygon
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Get the exterior ring (first ring)
    pub fn exterior(&self) -> Option<&Ring> {
        self.0.first()
    }

    /// Get the interior rings (all rings except the first)
    pub fn interiors(&self) -> &[Ring] {
        if self.0.len() <= 1 {
            &[]
        } else {
            &self.0[1..]
        }
    }

    /// Calculate the area of the polygon
    pub fn area(&self) -> f64 {
        if self.0.is_empty() {
            return 0.0;
        }

        let mut area = 0.0;
        if let Some(exterior) = self.0.first() {
            area += exterior.area();
        }

        // Subtract interior areas
        for interior in self.interiors() {
            area -= interior.area();
        }

        area
    }

    /// Calculate the perimeter of the polygon
    pub fn perimeter(&self) -> f64 {
        self.0.iter().map(|ring| ring.perimeter()).sum()
    }

    /// Check if the polygon is valid
    pub fn is_valid(&self) -> bool {
        if self.0.is_empty() {
            return false;
        }

        // Check that all rings are valid
        for ring in &self.0 {
            if !ring.is_valid() {
                return false;
            }
        }

        // Check that the exterior ring has positive area
        if let Some(exterior) = self.0.first() {
            if exterior.area() <= 0.0 {
                return false;
            }
        }

        true
    }
}

impl MultiPolygon {
    /// Create a new empty multi-polygon
    pub fn new() -> Self {
        MultiPolygon(Vec::new())
    }

    /// Create a new multi-polygon from polygons
    pub fn from_polygons(polygons: Vec<Polygon>) -> Self {
        MultiPolygon(polygons)
    }

    /// Create a new multi-polygon with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        MultiPolygon(Vec::with_capacity(capacity))
    }

    /// Get the number of polygons
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the multi-polygon is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get a polygon by index
    pub fn get(&self, index: usize) -> Option<&Polygon> {
        self.0.get(index)
    }

    /// Get a mutable polygon by index
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Polygon> {
        self.0.get_mut(index)
    }

    /// Add a polygon
    pub fn push(&mut self, polygon: Polygon) {
        self.0.push(polygon);
    }

    /// Remove the last polygon
    pub fn pop(&mut self) -> Option<Polygon> {
        self.0.pop()
    }

    /// Insert a polygon at the specified index
    pub fn insert(&mut self, index: usize, polygon: Polygon) {
        self.0.insert(index, polygon);
    }

    /// Remove a polygon at the specified index
    pub fn remove(&mut self, index: usize) -> Polygon {
        self.0.remove(index)
    }

    /// Clear all polygons
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Calculate the total area of all polygons
    pub fn area(&self) -> f64 {
        self.0.iter().map(|polygon| polygon.area()).sum()
    }

    /// Calculate the total perimeter of all polygons
    pub fn perimeter(&self) -> f64 {
        self.0.iter().map(|polygon| polygon.perimeter()).sum()
    }

    /// Check if the multi-polygon is valid
    pub fn is_valid(&self) -> bool {
        self.0.iter().all(|polygon| polygon.is_valid())
    }
}

// Implement Display for all geometric types
impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.0, self.1)
    }
}

impl fmt::Display for Ring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, point) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", point)?;
        }
        write!(f, "]")
    }
}

impl fmt::Display for Polygon {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for (i, ring) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", ring)?;
        }
        write!(f, ")")
    }
}

impl fmt::Display for MultiPolygon {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, polygon) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", polygon)?;
        }
        write!(f, "]")
    }
}

// Implement From traits for conversions
impl From<(f64, f64)> for Point {
    fn from((x, y): (f64, f64)) -> Self {
        Point(x, y)
    }
}

impl From<Vec<Point>> for Ring {
    fn from(points: Vec<Point>) -> Self {
        Ring(points)
    }
}

impl From<Vec<Ring>> for Polygon {
    fn from(rings: Vec<Ring>) -> Self {
        Polygon(rings)
    }
}

impl From<Vec<Polygon>> for MultiPolygon {
    fn from(polygons: Vec<Polygon>) -> Self {
        MultiPolygon(polygons)
    }
}

// Implement TryFrom for Value conversions
impl TryFrom<Value> for Point {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Tuple(values) => {
                if values.len() == 2 {
                    let x = match &values[0] {
                        Value::Float32(f) => *f as f64,
                        Value::Float64(f) => *f,
                        Value::UInt8(u) => *u as f64,
                        Value::UInt16(u) => *u as f64,
                        Value::UInt32(u) => *u as f64,
                        Value::UInt64(u) => *u as f64,
                        Value::Int8(i) => *i as f64,
                        Value::Int16(i) => *i as f64,
                        Value::Int32(i) => *i as f64,
                        Value::Int64(i) => *i as f64,
                        _ => return Err("First tuple element must be numeric".to_string()),
                    };
                    let y = match &values[1] {
                        Value::Float32(f) => *f as f64,
                        Value::Float64(f) => *f,
                        Value::UInt8(u) => *u as f64,
                        Value::UInt16(u) => *u as f64,
                        Value::UInt32(u) => *u as f64,
                        Value::UInt64(u) => *u as f64,
                        Value::Int8(i) => *i as f64,
                        Value::Int16(i) => *i as f64,
                        Value::Int32(i) => *i as f64,
                        Value::Int64(i) => *i as f64,
                        _ => return Err("Second tuple element must be numeric".to_string()),
                    };
                    Ok(Point(x, y))
                } else {
                    Err("Point tuple must have exactly 2 elements".to_string())
                }
            }
            Value::Array(values) => {
                if values.len() == 2 {
                    let x = match &values[0] {
                        Value::Float32(f) => *f as f64,
                        Value::Float64(f) => *f,
                        Value::UInt8(u) => *u as f64,
                        Value::UInt16(u) => *u as f64,
                        Value::UInt32(u) => *u as f64,
                        Value::UInt64(u) => *u as f64,
                        Value::Int8(i) => *i as f64,
                        Value::Int16(i) => *i as f64,
                        Value::Int32(i) => *i as f64,
                        Value::Int64(i) => *i as f64,
                        _ => return Err("First array element must be numeric".to_string()),
                    };
                    let y = match &values[1] {
                        Value::Float32(f) => *f as f64,
                        Value::Float64(f) => *f,
                        Value::UInt8(u) => *u as f64,
                        Value::UInt16(u) => *u as f64,
                        Value::UInt32(u) => *u as f64,
                        Value::UInt64(u) => *u as f64,
                        Value::Int8(i) => *i as f64,
                        Value::Int16(i) => *i as f64,
                        Value::Int32(i) => *i as f64,
                        Value::Int64(i) => *i as f64,
                        _ => return Err("Second array element must be numeric".to_string()),
                    };
                    Ok(Point(x, y))
                } else {
                    Err("Point array must have exactly 2 elements".to_string())
                }
            }
            _ => Err(format!("Cannot convert {} to Point", value.type_name())),
        }
    }
}

// Implement Default traits
impl Default for Point {
    fn default() -> Self {
        Point(0.0, 0.0)
    }
}

impl Default for Ring {
    fn default() -> Self {
        Ring::new()
    }
}

impl Default for Polygon {
    fn default() -> Self {
        Polygon::new()
    }
}

impl Default for MultiPolygon {
    fn default() -> Self {
        MultiPolygon::new()
    }
}

// Implement arithmetic operations for Point
impl std::ops::Add for Point {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        Point(self.0 + other.0, self.1 + other.1)
    }
}

impl std::ops::Sub for Point {
    type Output = Self;

    fn sub(self, other: Self) -> Self::Output {
        Point(self.0 - other.0, self.1 - other.1)
    }
}

impl std::ops::Mul<f64> for Point {
    type Output = Self;

    fn mul(self, scalar: f64) -> Self::Output {
        Point(self.0 * scalar, self.1 * scalar)
    }
}

impl std::ops::Div<f64> for Point {
    type Output = Self;

    fn div(self, scalar: f64) -> Self::Output {
        Point(self.0 / scalar, self.1 / scalar)
    }
}

// Implement comparison traits
impl PartialEq<(f64, f64)> for Point {
    fn eq(&self, other: &(f64, f64)) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

// Implement conversion traits
impl From<Point> for (f64, f64) {
    fn from(point: Point) -> Self {
        (point.0, point.1)
    }
}
