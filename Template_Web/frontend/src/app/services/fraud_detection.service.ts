import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, of } from 'rxjs';
import { catchError, tap } from 'rxjs/operators';

// Define interfaces for API responses
export interface FraudDetectionResult {
  fraudProbability: number;
  riskTier: string;
  isFraud: boolean;
  riskFactors?: string[];
  entityDetails?: any; // Add this property to support entity details
}

export interface Entity {
  id: string;
  name: string;
  type: string;
}

@Injectable({ providedIn: 'root' })
export class FraudDetectionService {
  // Base API URL - make sure this matches your Flask server
  private readonly apiUrl = 'http://localhost:5001/api';
  
  constructor(private http: HttpClient) {}

  getEntitiesByType(stakeholderType: string): Observable<Entity[]> {
    // Log the API call for debugging
    console.log(`Fetching entities for type: ${stakeholderType}`);
    
    return this.http.get<Entity[]>(`${this.apiUrl}/entities/${stakeholderType}`)
      .pipe(
        tap(data => console.log(`Received ${data.length} entities from API`)),
        catchError(error => {
          console.error(`Error fetching ${stakeholderType} entities:`, error);
          
          // Log more details about the error for debugging
          if (error.error instanceof ErrorEvent) {
            console.error('Client-side error:', error.error.message);
          } else {
            console.error(`Server-side error: ${error.status} ${error.statusText}`);
            console.error('Error body:', error.error);
          }
          
          // Fall back to mock data (keep this for development purposes)
          return of(this.getMockEntities(stakeholderType));
        })
      );
  }
  
  detectFraud(data: { entityId: string, entityType: string }): Observable<FraudDetectionResult> {
    console.log('Sending fraud detection request:', data);
    
    return this.http.post<FraudDetectionResult>(`${this.apiUrl}/detect-fraud`, data)
      .pipe(
        tap(result => console.log('Received fraud detection result:', result)),
        catchError(error => {
          console.error('Error calling fraud detection API:', error);
          
          // Log more details about the error
          if (error.error instanceof ErrorEvent) {
            console.error('Client-side error:', error.error.message);
          } else {
            console.error(`Server-side error: ${error.status} ${error.statusText}`);
            console.error('Error body:', error.error);
          }
          
          // Return mock data as fallback
          return of(this.getMockDetectionResult(data.entityId));
        })
      );
  }
  
  // Existing mock data methods (keep them for fallback)...
  private getMockEntities(stakeholderType: string): Entity[] {
    // Your existing method...
    const mockEntities: Entity[] = [];
    let namePrefix = '';
    let count = 5;
    
    switch(stakeholderType) {
      case 'players_agents':
        namePrefix = 'Agent';
        break;
      // Other cases...
    }
    
    for (let i = 1; i <= count; i++) {
      mockEntities.push({
        id: `${stakeholderType}_${i}`,
        name: `${namePrefix} ${i}`,
        type: stakeholderType
      });
    }
    
    return mockEntities;
  }
  
  private getMockDetectionResult(entityId: string): FraudDetectionResult {
    // Your existing method...
    const idSum = entityId.split('').reduce((sum, char) => sum + char.charCodeAt(0), 0);
    const probability = Math.min(100, Math.max(5, (idSum % 100)));
    
    let riskTier = 'LOW';
    if (probability > 80) riskTier = 'VERY HIGH';
    else if (probability > 60) riskTier = 'HIGH';
    else if (probability > 40) riskTier = 'MEDIUM';
    else if (probability > 20) riskTier = 'LOW';
    else riskTier = 'VERY LOW';
    
    return {
      fraudProbability: probability,
      riskTier: riskTier,
      isFraud: probability > 70
    };
  }
}