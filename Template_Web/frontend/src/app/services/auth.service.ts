import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable, from, of } from 'rxjs';
import { map, tap, catchError } from 'rxjs/operators';
import { account } from '../config/appwrite.config';
import { OAuthProvider } from 'appwrite';
@Injectable({
  providedIn: 'root'
})
export class AuthService {
  private currentUserSubject = new BehaviorSubject<any>(null);
  currentUser$ = this.currentUserSubject.asObservable();
  
  constructor() {
    // Check for existing session on load
    this.checkSession();
  }
  
  private checkSession() {
    from(account.get()).pipe(
      tap(user => {
        this.currentUserSubject.next(user);
      })
    ).subscribe({
      error: () => {
        // User is not logged in, do nothing
        console.log('No active session found');
      }
    });
  }
  
  signInWithGoogle(): void {
    account.createOAuth2Session(
      'google' as OAuthProvider, 
      `${window.location.origin}`,             // Success URL
      `${window.location.origin}/auth-failed`  // Failure URL
    );
  }
  
  signInWithFacebook(): void {
    account.createOAuth2Session(
      'facebook' as OAuthProvider, 
      `${window.location.origin}`, 
      `${window.location.origin}/auth-failed`
    );
  }
  
signInWithLinkedIn(): void {
  account.createOAuth2Session(
    'linkedin' as OAuthProvider, // Add a space between 'linkedin' and 'as'
    `${window.location.origin}`,
    `${window.location.origin}/auth-failed`
  );
}
  
  // Get current user
  getCurrentUser(): Observable<any> {
    return from(account.get()).pipe(
      tap(user => this.currentUserSubject.next(user))
    );
  }
  processAuthToken(provider: string, code: string): Observable<any> {
  // For Appwrite, we don't need to process the token manually
  // as Appwrite handles the OAuth flow automatically
  // Just return user information after successful login
  return from(account.get()).pipe(
    tap(user => {
      this.currentUserSubject.next(user);
    }),
    catchError(error => {
      console.error('Error processing auth token:', error);
      return of(null);
    })
  );
}
  
  // Logout
  logout(): Observable<any> {
    return from(account.deleteSession('current')).pipe(
      tap(() => this.currentUserSubject.next(null))
    );
  }
  
  // Check if user is authenticated
  get isAuthenticated(): boolean {
    return this.currentUserSubject.value !== null;
  }
}