import { TestBed } from '@angular/core/testing';

import { ExportState } from './export.state';

describe('ExportState', () => {
  let service: ExportState;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(ExportState);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
