import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PresetExport } from './preset-export';

describe('PresetExport', () => {
  let component: PresetExport;
  let fixture: ComponentFixture<PresetExport>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PresetExport],
    }).compileComponents();

    fixture = TestBed.createComponent(PresetExport);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
