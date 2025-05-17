import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class NavigationService {
  // Default view is 'home'
  private currentViewSubject = new BehaviorSubject<string>('home');
  currentView$ = this.currentViewSubject.asObservable();
  
  constructor() {
    // Check for hash in URL on load
    this.handleHashChange();
    
    // Listen for hash changes
    window.addEventListener('hashchange', () => {
      this.handleHashChange();
    });
  }
  
  private handleHashChange() {
    const hash = window.location.hash.slice(1) || 'home';
    this.navigate(hash);
  }
  
  navigate(view: string) {
    this.currentViewSubject.next(view);
    window.location.hash = view;
  }
}